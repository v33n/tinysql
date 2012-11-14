%{
  #include <algorithm>
  #include <cstdio>
  #include <cstdlib>
  #include <cstring>
  #include <vector>
  #include <stack>
  #include <string>
  #include <sstream>
  #include <iostream>
  #include "Block.h"
  #include "Config.h"
  #include "Disk.h"
  #include "Field.h"
  #include "MainMemory.h"
  #include "Relation.h"
  #include "Schema.h"
  #include "SchemaManager.h"
  #include "Tuple.h"
  #include "Join.h"
  #include "DuplicateElimination.h"
  #include "OrderBy.h"
  #include "TwoPassJoin.h"

  //declared for C++ compiler
  extern "C"
  {
    int yyparse(void);
    int yylex(void);
    int yywrap() {
      return 1;
    }
    void yyerror(const char *str) {
      fprintf(stderr,"error: %s\n", str);
    }
  }

  extern int yydebug;
  extern FILE * yyin;    

  //global variables
  MainMemory mem;
  Disk disk;
  SchemaManager schema_manager(&mem, &disk);

  vector<enum FIELD_TYPE> field_types;
  vector<string> field_names;
  vector<string> insert_field_names; // field name vector only for insert
  vector<string> field_values;
  vector<string> table_names;
  vector<string> from_table_names;
  string where_clause = "";
  bool insert_via_select = false;
  Relation *insert_via_select_relation_ptr = NULL;
  string order_by_column = "";

  //main function
  main(int argc, char **argv) {
    if (argc > 1)
      yyin = fopen(argv[1], "r");
    else
      yyin = stdin;
    yyparse();
  }


  void appendTupleToRelation(Relation* relation_ptr, int memory_block_index, Tuple& tuple) {
    Block* block_ptr;
    if (relation_ptr->getNumOfBlocks() == 0) {
      block_ptr = mem.getBlock(memory_block_index);
      // Clear the block
      block_ptr->clear();
      // Append the tuple
      block_ptr->appendTuple(tuple);
      relation_ptr->setBlock(relation_ptr->getNumOfBlocks(), memory_block_index);
    } else {
      // Read the last block of the relation into memory block
      relation_ptr->getBlock(relation_ptr->getNumOfBlocks()-1, memory_block_index);
      block_ptr = mem.getBlock(memory_block_index);
      if (block_ptr->isFull()) {
	// The block is full: Clear the memory block and append the tuple
	block_ptr->clear();
	block_ptr->appendTuple(tuple);
	// Write to a new block at the end of the relation
	relation_ptr->setBlock(relation_ptr->getNumOfBlocks(), memory_block_index);
      } else {
	// The block is not full: Append it directly
	block_ptr->appendTuple(tuple);
	// Write to the last block of the relation
	relation_ptr->setBlock(relation_ptr->getNumOfBlocks()-1, memory_block_index);
      }
    }  
  }

  void displayRelationInfo(Relation *relation_ptr) {
    // Print the information about the Relation
    cout << "\nThe table has name " << relation_ptr->getRelationName();
    cout << "\nThe table has schema:\n" << relation_ptr->getSchema();
    cout << "\nThe table currently has: " << relation_ptr->getNumOfBlocks() << " blocks";
    cout << "\nThe table currently has: " << relation_ptr->getNumOfTuples() << " tuples" << "\n";
  }

  void displayRelationFields(Relation *relation_ptr) {
    // Print the information about the Relation fields
    vector<string> field_names = (relation_ptr->getSchema()).getFieldNames();
    cout << "\n";
    for (int i = 0; i < field_names.size(); i++)
      cout << field_names[i] << "\t";
    cout << "\n";
  }


  void displayRelation(Relation *relation_ptr) {
    Schema schema = relation_ptr->getSchema();
    Block *block_ptr = mem.getBlock(0);
    int num_blocks = relation_ptr->getNumOfBlocks();
    enum FIELD_TYPE field_type;

    cout << "\n";
    for (int i = 0; i < field_names.size(); i++)
      cout << field_names[i] << "\t";
    cout << "\n";

    for (int i = 0; i < num_blocks; i++) {
      block_ptr->clear();
      relation_ptr->getBlock(i, 0);
      vector<Tuple> tuples = block_ptr->getTuples();
        for (int j = 0; j < tuples.size(); j++) {
          if (tuples[j].isNull())
          continue;
          for (int k = 0; k < field_names.size(); k++) {
            field_type = schema.getFieldType(field_names[k]);
            if (field_type == INT)
              cout << tuples[j].getField(field_names[k]).integer << "\t";
	    else
              cout << *(tuples[j].getField(field_names[k]).str) << "\t";
          }
          cout << "\n";
	}
    }
    cout << "\n\n";
  }
      

  Relation * onePassProjection(Relation *relation_ptr, bool isStripDot) {
    int pos;
    Relation *new_relation_ptr;
    Block *block_ptr;
    Schema s = relation_ptr->getSchema();

    // Get projected columns info
    for (int i = 0; i < field_names.size(); i++) {
      if (isStripDot) {
        pos = field_names[i].find_first_of('.');
        if (pos != string::npos)
          field_names[i] = field_names[i].substr(pos + 1);
      }
      field_types.push_back(s.getFieldType(field_names[i]));
    }

    int num_blocks = relation_ptr->getNumOfBlocks();
    if (!num_blocks)
      return NULL;

    new_relation_ptr = schema_manager.createRelation(relation_ptr->getRelationName() + "_project",
						     Schema(field_names, field_types));
    
    // Use a single block of memory to display data  
    int num_tuples;
    block_ptr = mem.getBlock(0);

    for (int i = 0; i < num_blocks; i++) {
      block_ptr->clear();    
      relation_ptr->getBlock(i, 0);
      vector<Tuple> tuples = block_ptr->getTuples();
      for (int j = 0; j < tuples.size(); j++) {
        if (tuples[j].isNull())
          continue;
        Tuple tuple = new_relation_ptr->createTuple();
        for (int k = 0; k < field_names.size(); k++) {
          if (field_types[k] == INT)
            tuple.setField(field_names[k], tuples[j].getField(field_names[k]).integer);
	  else
            tuple.setField(field_names[k], *(tuples[j].getField(field_names[k]).str));
	}
        appendTupleToRelation(new_relation_ptr, 9, tuple);
      }
    }
    return new_relation_ptr;      
  }

  void splitTableAndColumnName(string name) {
    /*
    string table = "", column = name;
    int pos = name.find_first_of('.');
    if (pos != string::npos) {
      table = name.substr(0, pos);
      column = name.substr(pos + 1);
    }
    table_names.push_back(table);
    field_names.push_back(column);
    */
    field_names.push_back(name);
  }


  bool isOperator(string token) {
    if (token == "+" || token == "-" || token == "*" ||
        token == "/" || token == "&" || token == "|" ||
        token == "!" || token == "(" || token == ")" ||
        token == ")" || token == "[" || token == "]" ||
        token == "<" || token == ">" || token == "=")
      return true;
    return false;
  }


  int Precedence(string op) {
    if (op == "*" || op == "/")
      return 6;

    if (op == "+" || op == "-")
      return 5;

    if (op == ">" || op == "<" || op == "=")
      return 4;

    if (op == "!")
      return 3;

    if (op == "&")
      return 2;

    if (op == "|")
      return 1;

    if (op == "(" || op == ")" || op == "[" || op == "]")
      return 0;
  }

  string InfixToPostfix(string infix_clause) {
    stack<string> s;
    string postfix_clause = "", token;
    istringstream iss(infix_clause);

    while (iss >> token) {
      if (isOperator(token)) {
        if (s.empty() || Precedence(token) >= Precedence(s.top()) || token == "(" || token == "[") {
	  s.push(token);
        }
	else {
          while (!s.empty() && Precedence(s.top()) > Precedence(token)) {
            postfix_clause = postfix_clause + s.top() + " ";
            s.pop();
          }
          if (token == ")" || token == "]")
            s.pop();
          else
            s.push(token); 
	}
      }
      else
        postfix_clause = postfix_clause + token + " ";
    }

    while (!s.empty()) {
      postfix_clause = postfix_clause + s.top() + " ";
      s.pop();
    }

    return postfix_clause;
  }


  string getOperandValueForTuple(Schema &schema, Tuple &tuple, string operand) {
    if (!schema.fieldNameExists(operand))
      return operand;
    if (schema.getFieldType(operand) == INT) {
      int val = tuple.getField(operand).integer;    
      stringstream s;
      s << val;
      return s.str();
    }         
    return *(tuple.getField(operand).str);
  }

  bool evaluatePostfixForTuple(Schema &schema, Tuple &tuple, string &postfix_clause) {
    istringstream iss(postfix_clause);
    stack<string> result;
    string token, operand1, operand2;
    int val;

    while (iss >> token) {
      if (isOperator(token)) {
        stringstream s;
	// Get operands
        operand2 = getOperandValueForTuple(schema, tuple, result.top());
        result.pop();
        if (token != "!") {
          operand1 = getOperandValueForTuple(schema, tuple, result.top());
          result.pop();
        }
        // Compute value and push to stack

        switch(token[0]) {
	  case '+':
            val = atoi(operand1.c_str()) + atoi(operand2.c_str());
            s << val;
            result.push(s.str());
            break;
	  case '-':
            val = atoi(operand1.c_str()) - atoi(operand2.c_str());
            s << val;
            result.push(s.str());
            break;
	  case '*':
            val = atoi(operand1.c_str()) * atoi(operand2.c_str());
            s << val;
            result.push(s.str());
            break;
	  case '/':
            val = atoi(operand1.c_str()) / atoi(operand2.c_str());
            s << val;
            result.push(s.str());
            break;
  	  case '<':
            val = atoi(operand1.c_str()) < atoi(operand2.c_str());
            result.push(val? "1": "0");
            break;
  	  case '>':
            val = atoi(operand1.c_str()) > atoi(operand2.c_str());
            result.push(val? "1": "0");
            break;
  	  case '=':
            result.push((operand1 == operand2)? "1": "0");
            break;
  	  case '&':
            val = atoi(operand1.c_str()) & atoi(operand2.c_str());
            s << val;
            result.push(s.str());
            break;
  	  case '|':
            val = atoi(operand1.c_str()) | atoi(operand2.c_str());
            s << val;
            result.push(s.str());
            break;
          case '!':
            val = (atoi(operand2.c_str())? 0 : 1);
            s << val;
            result.push(s.str());
            break;
        }
      }
      else
        result.push(token);
    }
    return (atoi(result.top().c_str())? true: false);
  }

  Relation * evaluateForSingleRelation(Relation *relation_ptr, Schema &schema, string postfix_clause, bool isStripDot) {
    bool temp, isSingleRelation = false;
    Relation *new_relation_ptr;
    Block *block_ptr;
    int pos;

    // Get columns info
    for (int i = 0; i < field_names.size(); i++) {
      if (isStripDot) {
        pos = field_names[i].find_first_of('.');
        if (pos != string::npos)
          field_names[i] = field_names[i].substr(pos + 1);
      }
      field_types.push_back(schema.getFieldType(field_names[i]));
    }

    int num_blocks = relation_ptr->getNumOfBlocks();
    if (!num_blocks)
      return NULL;

    new_relation_ptr = schema_manager.createRelation(relation_ptr->getRelationName() + "_project",
						     Schema(field_names, field_types));

    // Use a single block of memory to display data  
    int num_tuples;
    block_ptr = mem.getBlock(0);

    for (int i = 0; i < num_blocks; i++) {
      block_ptr->clear();    
      relation_ptr->getBlock(i, 0);
      vector<Tuple> tuples = block_ptr->getTuples();
      for (int j = 0; j < tuples.size(); j++) {
        if (tuples[j].isNull())
          continue;
        temp = evaluatePostfixForTuple(schema, tuples[j], postfix_clause);
        if (!temp)
          continue;
        Tuple tuple = new_relation_ptr->createTuple();
        for (int k = 0; k < field_names.size(); k++) {
          if (field_types[k] == INT)
            tuple.setField(field_names[k], tuples[j].getField(field_names[k]).integer);
	  else
            tuple.setField(field_names[k], *(tuples[j].getField(field_names[k]).str));
	}
        appendTupleToRelation(new_relation_ptr, 9, tuple);
      }
    }      
    return new_relation_ptr;      
  }


  bool isJoin(string operand1, string operand2, string rel_operator) {
    // If either operand is a constant, its not  join
    // This condition is for balancing string::npos condition
    if (operand1 == "0" || operand2 == "0")
      return false;
    if (atoi(operand1.c_str()) || atoi(operand2.c_str()))
      return false;
    if (operand1.find("\"") != string::npos || operand2.find("\"") != string::npos)
      return false;
    return true;
  }

  void identifyConjunctiveSimplePredicates(vector<string> &SCP, vector<string> &simple_joins,
                                           string &where_clause_minus_scp) {
    bool isNot;
    int num_complex_clause = 0;

    // Using the where clause in the global variable
    istringstream iss(where_clause);

    string token, operand1, operand2, rel_operator, prev_condition, next_condition;
    vector<string> tokens;
    tokens.push_back("");

    while (iss >> token) {
      tokens.push_back(token);

      if (token == "(" || token == "[") {
        num_complex_clause++;
        continue;
      }
      if (token == ")" || token == "]") {
        num_complex_clause--;
        continue;
      }

      if (num_complex_clause == 0) {
        // This is a simple clause
        if (token == "=" || token == ">" || token == "<") {

          //Check if this is a simple conjunctive clause
          prev_condition = tokens[tokens.size() - 3];

          isNot = false;
          if (prev_condition == "!") {
            prev_condition = tokens[tokens.size() - 4];
            isNot = true;
          }

          if ( prev_condition != "&" && prev_condition != "")
            continue;

          operand1 = tokens[tokens.size() - 2];
          rel_operator = token;
          iss >> token;
          tokens.push_back(token);
          operand2 = token;

          // If this is the first SCP, you need to check next condition
          if (prev_condition == "") {
            if (iss >> token) {
              tokens.push_back(token);         
              next_condition = token;
              if (next_condition != "&")
                continue;
            }
	  }

          // If this is a NOT SCP, reverse relational operator
          if (isNot) {
            switch(rel_operator[0]) {
	    case '=': rel_operator = "$"; break;
	    case '>': rel_operator = "<"; break;
	    case '<': rel_operator = ">"; break; 
	    }
	  }

          // Check if not a join
	  if (!isJoin(operand1, operand2, rel_operator))
            SCP.push_back(operand1 + " " + rel_operator + " " + operand2);
          else {
            // Only push equi join
            if (rel_operator != "=")
              continue;
            simple_joins.push_back(operand1 + " " + rel_operator + " " + operand2);
	  }

           // Delete the SCP or simple join for forming the new where clause
	  if (prev_condition == "")
            tokens.pop_back(); // Pop next condition
           tokens.pop_back(); // operand2
           tokens.pop_back(); // relational operator
           tokens.pop_back(); // operand1
           if (isNot)
             tokens.pop_back();
           if (!tokens.empty() && tokens[tokens.size() - 1] == "&")
             tokens.pop_back();
	   if (prev_condition == "")
             tokens.push_back(next_condition); // Push next condition after deleting scp or simple join
	}
      }
    }

    // Erase the empty token pushed at the begining
    tokens.erase(tokens.begin());

    // SPecial case, there might be a & left at the very begining
    if (tokens[0] == "&")
      tokens.erase(tokens.begin());

    for (int i = 0; i < tokens.size(); i++)
      where_clause_minus_scp = where_clause_minus_scp + tokens[i] + " ";
  }

  map<string, string> combineSCPPerRelation(vector<string> SCP) {
    // Assume that when multiple relations are present, each column is dot prefixed by relation name
    // Assume operand on RHS is the constant one in SCP
    int pos;
    string relation_name, clause;
    map<string, string> relation_to_SCP;

    for (int i = 0; i < SCP.size(); i++) {
      pos = SCP[i].find(".");
      relation_name = SCP[i].substr(0, pos);
      clause = SCP[i].substr(pos + 1);
      if (relation_to_SCP.find(relation_name) == relation_to_SCP.end())
        relation_to_SCP[relation_name] = clause;
      else
        relation_to_SCP[relation_name] = relation_to_SCP[relation_name] + " & " + clause;        
    }
    return relation_to_SCP;
  }


  void pushDownSelectionForSingleRelation(Relation *old_relation_ptr,
                                           Relation *new_relation_ptr,
                                          Schema schema,
                                          string postfix_scp) {
    bool temp;
    Block *block_ptr;
    int num_blocks = old_relation_ptr->getNumOfBlocks();
    if (!num_blocks)
      return;

    // Use a single block of memory to copy data to temporary relation  
    int num_tuples;
    block_ptr = mem.getBlock(0);
    for (int i = 0; i < num_blocks; i++) {
      block_ptr->clear();    
      old_relation_ptr->getBlock(i, 0);
      vector<Tuple> tuples = block_ptr->getTuples();
      for (int j = 0; j < tuples.size(); j++) {
        temp = evaluatePostfixForTuple(schema, tuples[j], postfix_scp);
        if (temp)
          appendTupleToRelation(new_relation_ptr, 0, tuples[j]);
          
      }
    }    
  }        


  map<string, Relation *> pushDownSelection(map<string, string> relation_to_SCP) {
    Relation *old_relation_ptr, *new_relation_ptr;
    Schema schema;
    map<string, Relation *> old_to_new_relation_map;
    string old_relation_name, new_relation_name, scp_clauses, postfix_scp;

    for (map<string, string>::const_iterator it = relation_to_SCP.begin(); it != relation_to_SCP.end(); ++it) {
      old_relation_name = it->first;
      old_relation_ptr = schema_manager.getRelation(old_relation_name);
      schema = old_relation_ptr->getSchema();

      scp_clauses = it->second;
      new_relation_name = old_relation_name + "_temp";

      // Create a new temporary relation
      new_relation_ptr = schema_manager.createRelation(new_relation_name,
                                                       Schema(schema.getFieldNames(),
                                                       schema.getFieldTypes()));

      old_to_new_relation_map[old_relation_name] = new_relation_ptr;

      // Convert SCP clause to postfix notation
      postfix_scp = InfixToPostfix(scp_clauses);

      // Apply selection to this table
      pushDownSelectionForSingleRelation(old_relation_ptr, new_relation_ptr, schema, postfix_scp);
    }
    return old_to_new_relation_map;
  }


  map< pair<string, string>, string > combineJoinsPerRelation(vector<string> &simple_joins) {
    int pos;
    string operand1, operand2, relation_name1, rel_operator, relation_name2, field1, field2;
    map< pair<string, string>, string> relations_to_simple_joins;
    pair<string, string> key;

    for (int i = 0; i < simple_joins.size(); i++) {
      istringstream iss(simple_joins[i]);
      iss >> operand1;
      iss >> rel_operator;
      iss >> operand2;

      pos = operand1.find(".");
      relation_name1 = operand1.substr(0, pos);
      field1 = operand1.substr(pos + 1);

      pos = operand2.find(".");
      relation_name2 = operand2.substr(0, pos);
      field2 = operand2.substr(pos + 1);

      key = pair<string, string> (relation_name1, relation_name2);
      if (relation_name1 > relation_name2)
        key = pair<string, string> (relation_name2, relation_name1);

      if (relations_to_simple_joins.find(key) == relations_to_simple_joins.end())
        relations_to_simple_joins[key] = simple_joins[i];
      else
        relations_to_simple_joins[key] = relations_to_simple_joins[key] + " & " + simple_joins[i];        
    }
    return relations_to_simple_joins;
  }

  void getOperatorsAndColumnsForJoin(string value, vector<string> &rel_operators,
                                     vector<string> &columns_relation1,
                                     vector<string> &columns_relation2) {
    istringstream iss(value);
    string token, field_name;
    int pos;

    while(iss >> token) {
      pos = token.find(".");
      field_name = token.substr(pos + 1);
      columns_relation1.push_back(field_name);

      iss >> token;
      rel_operators.push_back(token);

      iss >> token;
      pos = token.find(".");
      field_name = token.substr(pos + 1);
      columns_relation2.push_back(field_name);

      iss >> token;
    }
  }


  Relation * evaluateJoins(map<pair<string, string>, string> &relations_to_simple_joins,
                     map<string, Relation *> &old_to_new_relation_map) {
    pair<string, string> key;
    vector<string> relations, rel_operators, columns_relation1, columns_relation2;
    Relation *join, *relation_ptr1, *relation_ptr2;
    string value;
 
    for (map<pair<string, string>, string>::const_iterator it = relations_to_simple_joins.begin(); 
         it != relations_to_simple_joins.end(); ++it) {
      key = it->first;
      value = it->second;

      if (old_to_new_relation_map.find(key.first) == old_to_new_relation_map.end())
        relation_ptr1 = schema_manager.getRelation(key.first);
      else
        relation_ptr1 = old_to_new_relation_map[key.first];

      if (old_to_new_relation_map.find(key.second) == old_to_new_relation_map.end())
        relation_ptr2 = schema_manager.getRelation(key.second);
      else
        relation_ptr2 = old_to_new_relation_map[key.second];

      // fill in rel_operators and columns vector
      getOperatorsAndColumnsForJoin(value, rel_operators, columns_relation1, columns_relation2);

      // Call join operation
      join = joinRelations(relation_ptr1, relation_ptr2, columns_relation1, columns_relation2, rel_operators,
                           "NaturalJoin", key.first, key.second);
    }
    return join;
  }

  void getJoinInfo(string simple_join, string &relation_name1, string &relation_name2,
                   vector<string> &columns, vector<string> &rel_operators) {
    string operand1, operand2, rel_operator;
    int pos;
    istringstream iss(simple_join);
    iss >> operand1;
    iss >> rel_operator;
    iss >> operand2;

    pos = operand1.find(".");
    relation_name1 = operand1.substr(0, pos);

    columns.push_back(operand1.substr(pos + 1));

    pos = operand2.find(".");
    relation_name2 = operand2.substr(0, pos);
      
    rel_operators.push_back(rel_operator);
  }

  string getNextJoin(vector<string> &simple_joins, string relation_name1, string relation_name2) {
    string r1, r2, simple_join;
    vector<string> cols, rel_op;
    for (int i = 0; i < simple_joins.size(); i++) {
      getJoinInfo(simple_joins[i], r1, r2, cols, rel_op);      
      if ( (r1 == relation_name1 && r2 == relation_name2) ||
           (r2 == relation_name1 && r1 == relation_name2)) {
        simple_join = simple_joins[i];
        simple_joins.erase(simple_joins.begin() + i);
        return simple_join;
      }
    }
    // If no join found, resturn empty string
    return "";
  }

  bool isSwapRelations(string &relation_name1, string &relation_name2,
                       Relation *relation_ptr1, Relation *relation_ptr2) {
    if (relation_name1 == relation_ptr1->getRelationName())
      relation_name1 = relation_ptr2->getRelationName();
    else if (relation_name1 == relation_ptr2->getRelationName())
      relation_name1 = relation_ptr1->getRelationName();

    if (relation_name2 == relation_ptr1->getRelationName()) {
      relation_name2 = relation_ptr2->getRelationName();
      return true;
    }
    else if (relation_name2 == relation_ptr2->getRelationName()) {
      relation_name2 = relation_ptr1->getRelationName();
      return true;
    }
    return false;
  }

  Relation * optimizeAndEvaluateJoins(vector<string> simple_joins, map<string, Relation *> &old_to_new_relation_map,
                                      string rem_where_clause) {
    // Optimization and evaluation of 3 natural joins
    vector<Relation *> relation_ptrs;
    vector<string> columns, rel_operators;
    string relation_name1, relation_name2, simple_join;
    Relation *join, *temp, *R, *S;
    int i, j, k;

    // Fill in pointers after Pushdown Selection step
    for (i = 0; i < from_table_names.size(); i++) {
      if (old_to_new_relation_map.find(from_table_names[i]) == old_to_new_relation_map.end())
        relation_ptrs.push_back(schema_manager.getRelation(from_table_names[i]));
      else
        relation_ptrs.push_back(old_to_new_relation_map[from_table_names[i]]);
    }

    // Join Optimization: Sort relation by number of tuples
    for (i = 0; i < relation_ptrs.size(); i++) {
      for (j = 0; j < relation_ptrs.size(); j++) {
        if (i == j)
          continue;
        if (relation_ptrs[i]->getNumOfTuples() > relation_ptrs[j]->getNumOfTuples()) {
          temp = relation_ptrs[i];
          relation_ptrs[i] = relation_ptrs[j];
          relation_ptrs[j] = temp;
	}
      }
    }

    // For the first join, keep the column names as same
    simple_join = getNextJoin(simple_joins, relation_ptrs[0]->getRelationName(),
                              relation_ptrs[1]->getRelationName());
    getJoinInfo(simple_join, relation_name1, relation_name2, columns, rel_operators);
    R = relation_ptrs[0];
    S = relation_ptrs[1];

    if (isSwapRelations(relation_name1, relation_name2, relation_ptrs[0], relation_ptrs[1])) {
      S = relation_ptrs[0];
      R = relation_ptrs[1];
    }

    join = joinRelations(R, S, columns, columns, rel_operators,
                         R->getRelationName() + ".NaturalJoin." + S->getRelationName(), "", "");
    temp = join;

    // Second join
    columns.clear();
    rel_operators.clear();
    simple_join = getNextJoin(simple_joins, relation_ptrs[1]->getRelationName(),
                                relation_ptrs[2]->getRelationName());
    getJoinInfo(simple_join, relation_name1, relation_name2, columns, rel_operators);
    R = join;
    S = relation_ptrs[2];

    if (isSwapRelations(relation_name1, relation_name2, relation_ptrs[0], relation_ptrs[1])) {
      S = join;
      R = relation_ptrs[2];
    }
    join = joinRelations(R, S, columns, columns, rel_operators,
                         R->getRelationName() + ".NaturalJoin." + S->getRelationName(),
                         relation_name1, relation_name2);
    // Remove first join
    schema_manager.deleteRelation(temp->getRelationName());

    // Add last condition in remaining where clause
    if (rem_where_clause == "")
      rem_where_clause += simple_joins[0];
    else
      rem_where_clause += " & " + simple_joins[0];
    return join;
  }


  void removeTemporaryRelations(map<string, Relation *> &old_to_new_relation_map) {
    Relation *temp_relation_ptr;
    for (map<string, Relation *>::const_iterator it = old_to_new_relation_map.begin(); 
         it != old_to_new_relation_map.end(); ++it) {
      temp_relation_ptr = it->second;
      schema_manager.deleteRelation(temp_relation_ptr->getRelationName());
    }
  }


  void deleteTuplesFromRelation(Relation *relation_ptr, Schema schema) {
    int num_blocks, num_tuples;
    string postfix_clause = "";
    Block *block_ptr;

    if (where_clause == "") {
      relation_ptr->deleteBlocks(0);
      return;
    }

    postfix_clause = InfixToPostfix(where_clause);

    num_blocks = relation_ptr->getNumOfBlocks();
    if (!num_blocks)
      return;

    // Use a single block of memory to hold data  
    block_ptr = mem.getBlock(0);
    for (int i = 0; i < num_blocks; i++) {
      block_ptr->clear();    
      relation_ptr->getBlock(i, 0);
      num_tuples = schema.getTuplesPerBlock();
      for (int j = 0; j < num_tuples; j++) {
        Tuple t = block_ptr->getTuple(j);
        if (t.isNull())
          continue;
        if (evaluatePostfixForTuple(schema, t, postfix_clause))
          block_ptr->nullTuple(j);
      }
      relation_ptr->setBlock(i, 0);
    }
  }

  void getAllFieldNames(Schema &schema) {
    field_names.clear();
    field_names = schema.getFieldNames();
  }

  void clearGlobalVars() {
    // Clear the global variables
    field_names.clear();
    field_types.clear();
    field_values.clear();
    table_names.clear();
    from_table_names.clear();
    where_clause = "";
    insert_via_select = false;
    insert_via_select_relation_ptr = NULL;
    insert_field_names.clear();
    order_by_column = "";
  }

%}

  %token CREATE TABLE INSERT INTO VALUES DROP SELECT DISTINCT DELETE FROM WHERE ORDER BY AND OR NOT OPAREN EPAREN OPAREN_SQ EPAREN_SQ COMMA STAR PLUS MINUS DIV LT GT EQ NULLTOK NEWLINE

  %union {
    int number;
    char *str;
  }

  %token <str> NAME
  %token <number> INTTOK
  %token <number> STR20TOK
  %token <str> INTEGER
  %token <str> LITERAL
  %token <str> DOT_PREFIX_NAME
  %type <number> data_type
  %type <str> value
  %type <number> distinct_opt
  %type <number> where_clause_opt
  %type <number> order_by_clause_opt
%%

  // Grammar Rules

  // Start Rule: This rule ensures that the user can keep on entering Tiny SQL statements
  statements: statement NEWLINE | statements statement NEWLINE;

  statement: create_table_statement | insert_statement | drop_table_statement | select_statement | delete_statement;

  create_table_statement:
    CREATE TABLE NAME OPAREN attribute_type_list EPAREN {

    // Use the two global vector variables field_names, field_types to create a schema
    Schema schema(field_names, field_types);
    string relation_name = $3;
    Relation* relation_ptr = schema_manager.createRelation(relation_name, schema);

    displayRelationInfo(relation_ptr);

    clearGlobalVars();
  };

  attribute_type_list:
    NAME data_type {
      // Store the field name in vector field_names
      field_names.push_back($1);

      // Store the field type in vector field_types
      if ($2)   
        field_types.push_back(STR20);
      else
        field_types.push_back(INT);
    }
    | NAME data_type COMMA attribute_type_list {
      // Stores the field name in vector field_names
      field_names.push_back($1);

      // Store the field type in vector field_types
      if ($2)   
        field_types.push_back(STR20);
      else
        field_types.push_back(INT);
    };

  data_type:
    INTTOK {
      $$ = $1;
    } 
  | STR20TOK { 
      $$ = $1;
    };

  insert_statement:
    INSERT INTO NAME OPAREN attribute_list EPAREN insert_tuples {
      string relation_name = $3;
      if (!schema_manager.relationExists(relation_name)) {
        yyerror("Relation does not exist!");
        return 0;
      }
      Relation *relation_ptr = schema_manager.getRelation(relation_name);
      Schema schema = relation_ptr->getSchema();

      if (!insert_via_select) {
        Tuple tuple = relation_ptr->createTuple();
        // Fill the tuple object
        for (int i = 0; i < insert_field_names.size(); i++) {
          int field_type = tuple.getSchema().getFieldType(insert_field_names[i]);
          if (field_type == INT)
            tuple.setField(insert_field_names[i], atoi(field_values[i].c_str()));
          else
            tuple.setField(insert_field_names[i], field_values[i]);
        }
        // Write tuple to the relation using block 0 of main memory
        appendTupleToRelation(relation_ptr, 0, tuple);
      }
      else {
        if (insert_via_select_relation_ptr) {
          Block *block_ptr;
          int num_blocks = insert_via_select_relation_ptr->getNumOfBlocks();
          block_ptr = mem.getBlock(0);
          for (int i = 0; i < num_blocks; i++) {
            block_ptr->clear();
            insert_via_select_relation_ptr->getBlock(i, 0);
            vector<Tuple> tuples = block_ptr->getTuples();
            for (int j = 0; j < tuples.size(); j++) {
              if (tuples[j].isNull())
                continue;
              Tuple t = relation_ptr->createTuple(); 
              for (int k = 0; k < insert_field_names.size(); k++) {
                if (schema.getFieldType(insert_field_names[k]) == INT) 
                  t.setField(insert_field_names[k], tuples[j].getField(insert_field_names[k]).integer);
                else
                  t.setField(insert_field_names[k], *(tuples[j].getField(insert_field_names[k]).str));
	      }
              appendTupleToRelation(relation_ptr, 9, t);
	    }
	  }
          schema_manager.deleteRelation(insert_via_select_relation_ptr->getRelationName());
	}
      }
      
      cout << "\n" << *relation_ptr << "\n\n";
      clearGlobalVars();
  };

  attribute_list:
    NAME {
      // Stores the field name in vector field_names
       insert_field_names.push_back($1);  
    }
    | NAME COMMA attribute_list {
      // Stores the field name in vector field_names
      insert_field_names.push_back($1);
    };

  insert_tuples: VALUES OPAREN value_list EPAREN | insert_select_flag select_statement;

  insert_select_flag:
  {
    insert_via_select = true;
  }

  value_list:
    value {
      // Store the value in vector field_values
      field_values.push_back($1);
    } 
    | value COMMA value_list {
      // Store the value in vector field_values
      field_values.push_back($1);
    }

  value:
    LITERAL  {
      $$ = $1;
    }
    | INTEGER {
      $$ = $1;
    }
    | NULLTOK {
      $$ = strdup("NULL");
    };

  drop_table_statement:
    DROP TABLE NAME {
      string relation_name = $3;
      if (schema_manager.deleteRelation(relation_name))
        cout << "\nRelation " << relation_name << " just did a free fall. Successfully dropped!\n\n";
      clearGlobalVars();
    };

  delete_statement:
    DELETE FROM NAME where_clause_opt {
      string relation_name = $3;
      bool clearRelation = false;
      Relation *relation_ptr;
      Schema schema;
      relation_ptr = schema_manager.getRelation(relation_name);
      schema = relation_ptr->getSchema();

      // Use the global where clause
      deleteTuplesFromRelation(relation_ptr, schema);
      cout << "\nSuccessfully deleted records from " << relation_name << "! Trust me!\n\n";
      clearGlobalVars();
    }

  select_statement:
    SELECT distinct_opt select_list FROM table_list where_clause_opt order_by_clause_opt{

      Relation *new_relation_ptr = NULL, *dedup_relation_ptr = NULL, *ordered_relation_ptr = NULL,
 	       *projected_relation_ptr = NULL, *join = NULL;
      Schema join_schema;

      bool isDistinct = ($2 == 1)? true: false;
      bool isOrderBy = ($7 == 1)? true: false;

      bool isAllDistinct = false;
      bool isDistinctFirst = false;

      /* Evaluate cases of single relation here */

      // If Select * is called, get all column names for the relation
      if (from_table_names.size() == 1) {
        Relation *relation_ptr;
        Schema schema;
        relation_ptr = schema_manager.getRelation(from_table_names[0]);
        schema = relation_ptr->getSchema();
           
        if (field_names[0] == "*") {
          getAllFieldNames(schema);
          isAllDistinct = true;
        }

        // Check what has to be performed first: Duplicate Elimination or Order By
        if (isOrderBy && find(field_names.begin(), field_names.end(), order_by_column) != field_names.end())
          isDistinctFirst = true;

        // If no where clause, call one pass projection algorithm
        if (where_clause == "")
          new_relation_ptr = onePassProjection(relation_ptr, true);
        else {
          if (!isDistinctFirst && isOrderBy)
            field_names.push_back(order_by_column);

          // Convert where clause to postfix
          string postfix_where_clause = InfixToPostfix(where_clause);
          new_relation_ptr = evaluateForSingleRelation(relation_ptr, schema, postfix_where_clause, true);

          if (!isDistinctFirst && isOrderBy)
            field_names.pop_back();
	}
      }
      else {
        /* These are the cases of multiple relations */
        if (where_clause == "") {
	  // If no where clause, should be a cross join
          // Read the FROM clause to form a join
          Relation *relation_ptr1, *relation_ptr2;
          relation_ptr1 = schema_manager.getRelation(from_table_names[0]);
          relation_ptr2 = schema_manager.getRelation(from_table_names[1]);
          join = joinRelations(relation_ptr1, relation_ptr2, vector<string> (),
                               vector<string> (), vector<string> (),
                               relation_ptr1->getRelationName() + "." + relation_ptr2->getRelationName());

          for (int i = 2; i < from_table_names.size(); i++) {
            relation_ptr2 = schema_manager.getRelation(from_table_names[i]);
            relation_ptr1 = join;
            join = joinRelations(relation_ptr1, relation_ptr2, vector<string> (),
                                 vector<string> (), vector<string> (),
                                 relation_ptr1->getRelationName() + "." + relation_ptr2->getRelationName());
            schema_manager.deleteRelation(relation_ptr1->getRelationName());
	  }

          join_schema = join->getSchema();

          if (field_names[0] == "*") {
            getAllFieldNames(join_schema);
            isAllDistinct = true;
          }
          new_relation_ptr = onePassProjection(join, false);
	}
        else {
          // There is a where clause. Evaluate with Optimization.
          map<string, Relation *> old_to_new_relation_map;
          map< pair<string, string>, string> relations_to_simple_joins;
          vector<string> SCP, simple_joins;
          string rem_where_clause = "";

          // Parse where clause to identify simple conjunctive predicates and simple joins
          identifyConjunctiveSimplePredicates(SCP, simple_joins, rem_where_clause);

          // For each of these clause, shrink them to simple conjuctive clauses per relation
          if (!SCP.empty()) {
            map<string, string> relation_to_SCP = combineSCPPerRelation(SCP);

            // Push down selection
            old_to_new_relation_map = pushDownSelection(relation_to_SCP);
          }

          if (!simple_joins.empty()) {
            if (simple_joins.size() > 1)
              join = optimizeAndEvaluateJoins(simple_joins, old_to_new_relation_map, rem_where_clause);
            else {
              // For the simple join, shrink to simple conjuctive joins for relation pair
              relations_to_simple_joins = combineJoinsPerRelation(simple_joins);

              // Apply Joins now after push down optimization
              join = evaluateJoins(relations_to_simple_joins, old_to_new_relation_map);
	    }

            join_schema = join->getSchema();

            // Set up field names for * before evaluating final query
            if (field_names[0] == "*") {
              getAllFieldNames(join_schema);
              isAllDistinct = true;
            }

            // Check what has to be performed first: Duplicate Elimination or Order By
            if (isOrderBy && find(field_names.begin(), field_names.end(), order_by_column) != field_names.end())
              isDistinctFirst = true;

            // Remove Pushdown selection temporary tables if join is done
            removeTemporaryRelations(old_to_new_relation_map);
          }

          if (rem_where_clause != "") {
            // Evaluate query after optimization
            // a. Convert remaining where clause to postfix

            rem_where_clause = InfixToPostfix(rem_where_clause);

            // b. Evaluate query. The complex clause left should be
            //    evaluated on the join result.
            if (!isDistinctFirst && isOrderBy)
              field_names.push_back(order_by_column);

            new_relation_ptr = evaluateForSingleRelation(join, join_schema, rem_where_clause, false);

            if (!isDistinctFirst && isOrderBy)
              field_names.pop_back();

          }
          else
            new_relation_ptr = join;
          //else
	  //new_relation_ptr = onePassProjection(join, false);

          // Delete temporary relation for join
          // schema_manager.deleteRelation(join->getRelationName());
        }
      }

      if (!insert_via_select) {
        if (new_relation_ptr) {
          /*
          // Check what has to be performed first: Duplicate Elimination or Order By
          bool isDistinctFirst = false;
          if (isOrderBy && find(field_names.begin(), field_names.end(), order_by_column) != field_names.end())
            isDistinctFirst = true;
	  */
          if (isOrderBy && !isDistinctFirst) {
            // Check for ORDER BY
            enum FIELD_TYPE order_by_column_type = (new_relation_ptr->getSchema()).getFieldType(order_by_column);
            ordered_relation_ptr = orderByRelation(new_relation_ptr, order_by_column, order_by_column_type, true);
	  }

          // Can take One Pass projection if rem where clause was not evaluated
          projected_relation_ptr = new_relation_ptr;
          if (new_relation_ptr == join) {
            if (ordered_relation_ptr)
              projected_relation_ptr = onePassProjection(ordered_relation_ptr, false);
            else
              projected_relation_ptr = onePassProjection(join, false);
	  }
          
          dedup_relation_ptr = projected_relation_ptr;
          // Check for DISTINCT
          if (isDistinct)
            dedup_relation_ptr = removeDuplicates(projected_relation_ptr, field_names, isAllDistinct = true);

          // Check for ORDER BY if it did not have to be evaluated first
          if (isOrderBy && isDistinctFirst) {
            enum FIELD_TYPE order_by_column_type = (dedup_relation_ptr->getSchema()).getFieldType(order_by_column);
            orderByRelation(dedup_relation_ptr, order_by_column, order_by_column_type, false);
	  }
          else
            displayRelation(dedup_relation_ptr);
            //cout << "\n" << *dedup_relation_ptr << "\n\n";

          if(isOrderBy && !isDistinctFirst &&
             schema_manager.relationExists(ordered_relation_ptr->getRelationName()))
            schema_manager.deleteRelation(ordered_relation_ptr->getRelationName());

          if (schema_manager.relationExists(dedup_relation_ptr->getRelationName()))
            schema_manager.deleteRelation(dedup_relation_ptr->getRelationName());

          if (isDistinct && schema_manager.relationExists(projected_relation_ptr->getRelationName()))
            schema_manager.deleteRelation(projected_relation_ptr->getRelationName());

          if (projected_relation_ptr != new_relation_ptr &&
              schema_manager.relationExists(new_relation_ptr->getRelationName()))
            schema_manager.deleteRelation(new_relation_ptr->getRelationName());

          if (join && new_relation_ptr != join && schema_manager.relationExists(join->getRelationName()))
            schema_manager.deleteRelation(join->getRelationName());

          /*
          if(!isDistinctFirst && isOrderBy && schema_manager.relationExists(ordered_relation_ptr->getRelationName()))
            schema_manager.deleteRelation(ordered_relation_ptr->getRelationName());

          if (schema_manager.relationExists(dedup_relation_ptr->getRelationName()))
            schema_manager.deleteRelation(dedup_relation_ptr->getRelationName());

          cout << "\ntest\n";
          if (isDistinct && schema_manager.relationExists(projected_relation_ptr->getRelationName()))
            schema_manager.deleteRelation(projected_relation_ptr->getRelationName());

          cout << "\ntest2\n";

          if (projected_relation_ptr != new_relation_ptr && new_relation_ptr->)
            schema_manager.deleteRelation(new_relation_ptr->getRelationName());

          cout << "\ntest3\n";

          if (join && new_relation_ptr != join)
            schema_manager.deleteRelation(join->getRelationName());

          cout << "\ntest4\n";
          */
	}
        clearGlobalVars();
      }
      else
        insert_via_select_relation_ptr = new_relation_ptr;
    
    };

  distinct_opt: 
    {
      $$ = 0;
    }
    | DISTINCT {
      $$ = 1;
    };

  select_list: STAR {
    field_names.push_back("*");
    table_names.push_back("");  
  } | select_sublist;

  select_sublist: NAME {
    splitTableAndColumnName($1);
  } | DOT_PREFIX_NAME {
        splitTableAndColumnName($1);
  } | NAME COMMA select_sublist {
        splitTableAndColumnName($1);
  } | DOT_PREFIX_NAME COMMA select_sublist {
        splitTableAndColumnName($1);
  };
 
  table_list:
    NAME {
      from_table_names.push_back($1);
    }
    | NAME COMMA table_list {
      from_table_names.push_back($1);
    };

  where_clause_opt:
    {
      $$ = 0;
    }
    | WHERE search_condition {
      $$ = 1;
    };

  search_condition: boolean_term | boolean_term or search_condition;

  boolean_term: boolean_factor | boolean_factor and boolean_term;

  boolean_factor: boolean_primary | not boolean_primary;

  boolean_primary: comparison_predicate | oparen_sq search_condition eparen_sq;

  comparison_predicate: expression comp_op expression;

  comp_op:
    LT {
      where_clause += "< ";
    }| GT {
      where_clause += "> ";
    }| EQ {
      where_clause += "= ";
    };

  expression: term | term plus expression | term minus expression;

  term: factor | factor mul term | factor div term;

  factor:
    NAME {
      where_clause += string($1) + " ";
    }
    | DOT_PREFIX_NAME {
        where_clause += string($1) + " ";
      }
    | LITERAL {
        where_clause += string($1) + " ";
      }
    | INTEGER {
        where_clause += string($1) + " ";
      }
    | oparen expression eparen;

  order_by_clause_opt:
    {
      $$ = 0;
    }
    | ORDER BY NAME {
      $$ = 1;
      order_by_column = string($3);
    }
    | ORDER BY DOT_PREFIX_NAME {
      $$ = 1;
      order_by_column = string($3);
    };

  /* All tokens used for where clause */
  and:
    AND {
      where_clause += "& ";
    };

  or:
    OR { 
      where_clause += "| ";
    };

  not:
    NOT {
      where_clause += "! ";
    };

  oparen_sq:
    OPAREN_SQ { 
      where_clause += "[ ";
    };

  eparen_sq:
    EPAREN_SQ {
      where_clause += "] ";
    };

  oparen:
    OPAREN { 
      where_clause += "( ";
    };

  eparen:
    EPAREN {
      where_clause += ") ";
    };

  plus:
    PLUS {
      where_clause += "+ ";
    };

  minus:
    MINUS { 
      where_clause += "- ";
    };

  mul:
    STAR {
      where_clause += "* ";
    };

  div:
    DIV {
      where_clause += "/ ";
    };
