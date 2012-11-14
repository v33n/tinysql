/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison implementation for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.5"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 0



/* Copy the first part of user declarations.  */

/* Line 268 of yacc.c  */
#line 1 "TinySQL.y"

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



/* Line 268 of yacc.c  */
#line 955 "TinySQL.cc"

/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 1
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     CREATE = 258,
     TABLE = 259,
     INSERT = 260,
     INTO = 261,
     VALUES = 262,
     DROP = 263,
     SELECT = 264,
     DISTINCT = 265,
     DELETE = 266,
     FROM = 267,
     WHERE = 268,
     ORDER = 269,
     BY = 270,
     AND = 271,
     OR = 272,
     NOT = 273,
     OPAREN = 274,
     EPAREN = 275,
     OPAREN_SQ = 276,
     EPAREN_SQ = 277,
     COMMA = 278,
     STAR = 279,
     PLUS = 280,
     MINUS = 281,
     DIV = 282,
     LT = 283,
     GT = 284,
     EQ = 285,
     NULLTOK = 286,
     NEWLINE = 287,
     NAME = 288,
     INTTOK = 289,
     STR20TOK = 290,
     INTEGER = 291,
     LITERAL = 292,
     DOT_PREFIX_NAME = 293
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 293 of yacc.c  */
#line 886 "TinySQL.y"

    int number;
    char *str;
  


/* Line 293 of yacc.c  */
#line 1036 "TinySQL.cc"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif


/* Copy the second part of user declarations.  */


/* Line 343 of yacc.c  */
#line 1048 "TinySQL.cc"

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  19
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   104

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  39
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  41
/* YYNRULES -- Number of rules.  */
#define YYNRULES  76
/* YYNRULES -- Number of states.  */
#define YYNSTATES  125

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   293

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint8 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    14,    16,    18,    20,
      27,    30,    35,    37,    39,    47,    49,    53,    58,    61,
      62,    64,    68,    70,    72,    74,    78,    83,    91,    92,
      94,    96,    98,   100,   102,   106,   110,   112,   116,   117,
     120,   122,   126,   128,   132,   134,   137,   139,   143,   147,
     149,   151,   153,   155,   159,   163,   165,   169,   173,   175,
     177,   179,   181,   185,   186,   190,   194,   196,   198,   200,
     202,   204,   206,   208,   210,   212,   214
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int8 yyrhs[] =
{
      40,     0,    -1,    41,    32,    -1,    40,    41,    32,    -1,
      42,    -1,    45,    -1,    51,    -1,    53,    -1,    52,    -1,
       3,     4,    33,    19,    43,    20,    -1,    33,    44,    -1,
      33,    44,    23,    43,    -1,    34,    -1,    35,    -1,     5,
       6,    33,    19,    46,    20,    47,    -1,    33,    -1,    33,
      23,    46,    -1,     7,    19,    49,    20,    -1,    48,    53,
      -1,    -1,    50,    -1,    50,    23,    49,    -1,    37,    -1,
      36,    -1,    31,    -1,     8,     4,    33,    -1,    11,    12,
      33,    58,    -1,     9,    54,    55,    12,    57,    58,    68,
      -1,    -1,    10,    -1,    24,    -1,    56,    -1,    33,    -1,
      38,    -1,    33,    23,    56,    -1,    38,    23,    56,    -1,
      33,    -1,    33,    23,    57,    -1,    -1,    13,    59,    -1,
      60,    -1,    60,    70,    59,    -1,    61,    -1,    61,    69,
      60,    -1,    62,    -1,    71,    62,    -1,    63,    -1,    72,
      59,    73,    -1,    65,    64,    65,    -1,    28,    -1,    29,
      -1,    30,    -1,    66,    -1,    66,    76,    65,    -1,    66,
      77,    65,    -1,    67,    -1,    67,    78,    66,    -1,    67,
      79,    66,    -1,    33,    -1,    38,    -1,    37,    -1,    36,
      -1,    74,    65,    75,    -1,    -1,    14,    15,    33,    -1,
      14,    15,    38,    -1,    16,    -1,    17,    -1,    18,    -1,
      21,    -1,    22,    -1,    19,    -1,    20,    -1,    25,    -1,
      26,    -1,    24,    -1,    27,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   907,   907,   907,   909,   909,   909,   909,   909,   912,
     925,   935,   947,   950,   955,  1008,  1012,  1017,  1017,  1020,
    1025,  1029,  1035,  1038,  1041,  1046,  1054,  1069,  1301,  1304,
    1308,  1311,  1313,  1315,  1317,  1319,  1324,  1327,  1332,  1335,
    1339,  1339,  1341,  1341,  1343,  1343,  1345,  1345,  1347,  1350,
    1352,  1354,  1358,  1358,  1358,  1360,  1360,  1360,  1363,  1366,
    1369,  1372,  1375,  1378,  1381,  1385,  1392,  1397,  1402,  1407,
    1412,  1417,  1422,  1427,  1432,  1437,  1442
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "CREATE", "TABLE", "INSERT", "INTO",
  "VALUES", "DROP", "SELECT", "DISTINCT", "DELETE", "FROM", "WHERE",
  "ORDER", "BY", "AND", "OR", "NOT", "OPAREN", "EPAREN", "OPAREN_SQ",
  "EPAREN_SQ", "COMMA", "STAR", "PLUS", "MINUS", "DIV", "LT", "GT", "EQ",
  "NULLTOK", "NEWLINE", "NAME", "INTTOK", "STR20TOK", "INTEGER", "LITERAL",
  "DOT_PREFIX_NAME", "$accept", "statements", "statement",
  "create_table_statement", "attribute_type_list", "data_type",
  "insert_statement", "attribute_list", "insert_tuples",
  "insert_select_flag", "value_list", "value", "drop_table_statement",
  "delete_statement", "select_statement", "distinct_opt", "select_list",
  "select_sublist", "table_list", "where_clause_opt", "search_condition",
  "boolean_term", "boolean_factor", "boolean_primary",
  "comparison_predicate", "comp_op", "expression", "term", "factor",
  "order_by_clause_opt", "and", "or", "not", "oparen_sq", "eparen_sq",
  "oparen", "eparen", "plus", "minus", "mul", "div", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    39,    40,    40,    41,    41,    41,    41,    41,    42,
      43,    43,    44,    44,    45,    46,    46,    47,    47,    48,
      49,    49,    50,    50,    50,    51,    52,    53,    54,    54,
      55,    55,    56,    56,    56,    56,    57,    57,    58,    58,
      59,    59,    60,    60,    61,    61,    62,    62,    63,    64,
      64,    64,    65,    65,    65,    66,    66,    66,    67,    67,
      67,    67,    67,    68,    68,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,    78,    79
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     1,     1,     1,     1,     6,
       2,     4,     1,     1,     7,     1,     3,     4,     2,     0,
       1,     3,     1,     1,     1,     3,     4,     7,     0,     1,
       1,     1,     1,     1,     3,     3,     1,     3,     0,     2,
       1,     3,     1,     3,     1,     2,     1,     3,     3,     1,
       1,     1,     1,     3,     3,     1,     3,     3,     1,     1,
       1,     1,     3,     0,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,    28,     0,     0,     0,     4,     5,
       6,     8,     7,     0,     0,     0,    29,     0,     0,     1,
       0,     2,     0,     0,    25,    30,    32,    33,     0,    31,
      38,     3,     0,     0,     0,     0,     0,     0,    26,     0,
       0,    15,     0,    34,    35,    36,    38,    68,    71,    69,
      58,    61,    60,    59,    39,    40,    42,    44,    46,     0,
      52,    55,     0,     0,     0,    12,    13,    10,     9,     0,
      19,     0,    63,    67,     0,    66,     0,    49,    50,    51,
       0,    73,    74,     0,     0,    75,    76,     0,     0,    45,
       0,     0,     0,    16,     0,    14,     0,    37,     0,    27,
      41,    43,    48,    53,    54,    56,    57,    70,    47,    72,
      62,    11,     0,    18,     0,    24,    23,    22,     0,    20,
      64,    65,    17,     0,    21
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
      -1,     6,     7,     8,    40,    67,     9,    42,    95,    96,
     118,   119,    10,    11,    12,    17,    28,    29,    46,    38,
      54,    55,    56,    57,    58,    80,    59,    60,    61,    99,
      76,    74,    62,    63,   108,    64,   110,    83,    84,    87,
      88
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -62
static const yytype_int8 yypact[] =
{
      43,     0,     4,     2,    19,    41,    36,    -1,   -62,   -62,
     -62,   -62,   -62,   -19,     9,    35,   -62,   -16,    38,   -62,
      29,   -62,    50,    51,   -62,   -62,    26,    49,    61,   -62,
      62,   -62,    44,    45,   -26,   -26,    46,   -18,   -62,    25,
      54,    53,    60,   -62,   -62,    58,    62,   -62,   -62,   -62,
     -62,   -62,   -62,   -62,   -62,    65,    67,   -62,   -62,    28,
      37,    16,   -10,   -18,    -3,   -62,   -62,    63,   -62,    45,
      77,    46,    71,   -62,   -18,   -62,   -18,   -62,   -62,   -62,
      -3,   -62,   -62,    -3,    -3,   -62,   -62,    -3,    -3,   -62,
      66,    69,    44,   -62,    68,   -62,    81,   -62,    76,   -62,
     -62,   -62,   -62,   -62,   -62,   -62,   -62,   -62,   -62,   -62,
     -62,   -62,     1,   -62,    17,   -62,   -62,   -62,    72,    70,
     -62,   -62,   -62,     1,   -62
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -62,   -62,    88,   -62,     3,   -62,   -62,    27,   -62,   -62,
     -25,   -62,   -62,   -62,     5,   -62,   -62,    30,    31,    57,
     -61,    21,   -62,    42,   -62,   -62,   -59,   -21,   -62,   -62,
     -62,   -62,   -62,   -62,   -62,   -62,   -62,   -62,   -62,   -62,
     -62
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -1
static const yytype_uint8 yytable[] =
{
      47,    48,    90,    49,    13,    91,    15,    26,    25,    48,
      14,    49,    27,   100,    22,    50,    48,    26,    51,    52,
      53,   102,    27,    50,   103,   104,    51,    52,    53,    16,
      50,    21,   115,    51,    52,    53,    19,   116,   117,     1,
      85,     2,    23,    86,     3,     4,     1,     5,     2,    34,
     120,     3,     4,    18,     5,   121,    77,    78,    79,    65,
      66,    31,    81,    82,    43,    44,   105,   106,    24,    32,
      33,    30,    35,    36,    68,    37,    69,    39,    41,    45,
      70,    71,    73,    75,    94,    98,    92,   112,   107,   109,
       4,   114,   122,   123,    20,   111,    93,   101,   124,     0,
       0,   113,    97,    72,    89
};

#define yypact_value_is_default(yystate) \
  ((yystate) == (-62))

#define yytable_value_is_error(yytable_value) \
  YYID (0)

static const yytype_int8 yycheck[] =
{
      18,    19,    63,    21,     4,    64,     4,    33,    24,    19,
       6,    21,    38,    74,    33,    33,    19,    33,    36,    37,
      38,    80,    38,    33,    83,    84,    36,    37,    38,    10,
      33,    32,    31,    36,    37,    38,     0,    36,    37,     3,
      24,     5,    33,    27,     8,     9,     3,    11,     5,    23,
      33,     8,     9,    12,    11,    38,    28,    29,    30,    34,
      35,    32,    25,    26,    34,    35,    87,    88,    33,    19,
      19,    33,    23,    12,    20,    13,    23,    33,    33,    33,
      20,    23,    17,    16,     7,    14,    23,    19,    22,    20,
       9,    15,    20,    23,     6,    92,    69,    76,   123,    -1,
      -1,    96,    71,    46,    62
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     3,     5,     8,     9,    11,    40,    41,    42,    45,
      51,    52,    53,     4,     6,     4,    10,    54,    12,     0,
      41,    32,    33,    33,    33,    24,    33,    38,    55,    56,
      33,    32,    19,    19,    23,    23,    12,    13,    58,    33,
      43,    33,    46,    56,    56,    33,    57,    18,    19,    21,
      33,    36,    37,    38,    59,    60,    61,    62,    63,    65,
      66,    67,    71,    72,    74,    34,    35,    44,    20,    23,
      20,    23,    58,    17,    70,    16,    69,    28,    29,    30,
      64,    25,    26,    76,    77,    24,    27,    78,    79,    62,
      59,    65,    23,    46,     7,    47,    48,    57,    14,    68,
      59,    60,    65,    65,    65,    66,    66,    22,    73,    20,
      75,    43,    19,    53,    15,    31,    36,    37,    49,    50,
      33,    38,    20,    23,    49
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  However,
   YYFAIL appears to be in use.  Nevertheless, it is formally deprecated
   in Bison 2.4.2's NEWS entry, where a plan to phase it out is
   discussed.  */

#define YYFAIL		goto yyerrlab
#if defined YYFAIL
  /* This is here to suppress warnings from the GCC cpp's
     -Wunused-macros.  Normally we don't worry about that warning, but
     some users do, and we want to make it easy for users to remove
     YYFAIL uses, which will produce warnings from Bison 2.5.  */
#endif

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* This macro is provided for backward compatibility. */

#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (YYLEX_PARAM)
#else
# define YYLEX yylex ()
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  yy_symbol_value_print (yyoutput, yytype, yyvaluep);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, int yyrule)
#else
static void
yy_reduce_print (yyvsp, yyrule)
    YYSTYPE *yyvsp;
    int yyrule;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       		       );
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, Rule); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (0, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  YYSIZE_T yysize1;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = 0;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - Assume YYFAIL is not used.  It's too flawed to consider.  See
       <http://lists.gnu.org/archive/html/bison-patches/2009-12/msg00024.html>
       for details.  YYERROR is fine as it does not invoke this
       function.
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                yysize1 = yysize + yytnamerr (0, yytname[yyx]);
                if (! (yysize <= yysize1
                       && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                  return 2;
                yysize = yysize1;
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  yysize1 = yysize + yystrlen (yyformat);
  if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
    return 2;
  yysize = yysize1;

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep)
#else
static void
yydestruct (yymsg, yytype, yyvaluep)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
#endif
{
  YYUSE (yyvaluep);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {

      default:
	break;
    }
}


/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (void);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */


/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Number of syntax errors so far.  */
int yynerrs;


/*----------.
| yyparse.  |
`----------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void)
#else
int
yyparse ()

#endif
#endif
{
    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yystacksize);

	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;

  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 9:

/* Line 1806 of yacc.c  */
#line 912 "TinySQL.y"
    {

    // Use the two global vector variables field_names, field_types to create a schema
    Schema schema(field_names, field_types);
    string relation_name = (yyvsp[(3) - (6)].str);
    Relation* relation_ptr = schema_manager.createRelation(relation_name, schema);

    displayRelationInfo(relation_ptr);

    clearGlobalVars();
  }
    break;

  case 10:

/* Line 1806 of yacc.c  */
#line 925 "TinySQL.y"
    {
      // Store the field name in vector field_names
      field_names.push_back((yyvsp[(1) - (2)].str));

      // Store the field type in vector field_types
      if ((yyvsp[(2) - (2)].number))   
        field_types.push_back(STR20);
      else
        field_types.push_back(INT);
    }
    break;

  case 11:

/* Line 1806 of yacc.c  */
#line 935 "TinySQL.y"
    {
      // Stores the field name in vector field_names
      field_names.push_back((yyvsp[(1) - (4)].str));

      // Store the field type in vector field_types
      if ((yyvsp[(2) - (4)].number))   
        field_types.push_back(STR20);
      else
        field_types.push_back(INT);
    }
    break;

  case 12:

/* Line 1806 of yacc.c  */
#line 947 "TinySQL.y"
    {
      (yyval.number) = (yyvsp[(1) - (1)].number);
    }
    break;

  case 13:

/* Line 1806 of yacc.c  */
#line 950 "TinySQL.y"
    { 
      (yyval.number) = (yyvsp[(1) - (1)].number);
    }
    break;

  case 14:

/* Line 1806 of yacc.c  */
#line 955 "TinySQL.y"
    {
      string relation_name = (yyvsp[(3) - (7)].str);
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
  }
    break;

  case 15:

/* Line 1806 of yacc.c  */
#line 1008 "TinySQL.y"
    {
      // Stores the field name in vector field_names
       insert_field_names.push_back((yyvsp[(1) - (1)].str));  
    }
    break;

  case 16:

/* Line 1806 of yacc.c  */
#line 1012 "TinySQL.y"
    {
      // Stores the field name in vector field_names
      insert_field_names.push_back((yyvsp[(1) - (3)].str));
    }
    break;

  case 19:

/* Line 1806 of yacc.c  */
#line 1020 "TinySQL.y"
    {
    insert_via_select = true;
  }
    break;

  case 20:

/* Line 1806 of yacc.c  */
#line 1025 "TinySQL.y"
    {
      // Store the value in vector field_values
      field_values.push_back((yyvsp[(1) - (1)].str));
    }
    break;

  case 21:

/* Line 1806 of yacc.c  */
#line 1029 "TinySQL.y"
    {
      // Store the value in vector field_values
      field_values.push_back((yyvsp[(1) - (3)].str));
    }
    break;

  case 22:

/* Line 1806 of yacc.c  */
#line 1035 "TinySQL.y"
    {
      (yyval.str) = (yyvsp[(1) - (1)].str);
    }
    break;

  case 23:

/* Line 1806 of yacc.c  */
#line 1038 "TinySQL.y"
    {
      (yyval.str) = (yyvsp[(1) - (1)].str);
    }
    break;

  case 24:

/* Line 1806 of yacc.c  */
#line 1041 "TinySQL.y"
    {
      (yyval.str) = strdup("NULL");
    }
    break;

  case 25:

/* Line 1806 of yacc.c  */
#line 1046 "TinySQL.y"
    {
      string relation_name = (yyvsp[(3) - (3)].str);
      if (schema_manager.deleteRelation(relation_name))
        cout << "\nRelation " << relation_name << " just did a free fall. Successfully dropped!\n\n";
      clearGlobalVars();
    }
    break;

  case 26:

/* Line 1806 of yacc.c  */
#line 1054 "TinySQL.y"
    {
      string relation_name = (yyvsp[(3) - (4)].str);
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
    break;

  case 27:

/* Line 1806 of yacc.c  */
#line 1069 "TinySQL.y"
    {

      Relation *new_relation_ptr = NULL, *dedup_relation_ptr = NULL, *ordered_relation_ptr = NULL,
 	       *projected_relation_ptr = NULL, *join = NULL;
      Schema join_schema;

      bool isDistinct = ((yyvsp[(2) - (7)].number) == 1)? true: false;
      bool isOrderBy = ((yyvsp[(7) - (7)].number) == 1)? true: false;

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
    
    }
    break;

  case 28:

/* Line 1806 of yacc.c  */
#line 1301 "TinySQL.y"
    {
      (yyval.number) = 0;
    }
    break;

  case 29:

/* Line 1806 of yacc.c  */
#line 1304 "TinySQL.y"
    {
      (yyval.number) = 1;
    }
    break;

  case 30:

/* Line 1806 of yacc.c  */
#line 1308 "TinySQL.y"
    {
    field_names.push_back("*");
    table_names.push_back("");  
  }
    break;

  case 32:

/* Line 1806 of yacc.c  */
#line 1313 "TinySQL.y"
    {
    splitTableAndColumnName((yyvsp[(1) - (1)].str));
  }
    break;

  case 33:

/* Line 1806 of yacc.c  */
#line 1315 "TinySQL.y"
    {
        splitTableAndColumnName((yyvsp[(1) - (1)].str));
  }
    break;

  case 34:

/* Line 1806 of yacc.c  */
#line 1317 "TinySQL.y"
    {
        splitTableAndColumnName((yyvsp[(1) - (3)].str));
  }
    break;

  case 35:

/* Line 1806 of yacc.c  */
#line 1319 "TinySQL.y"
    {
        splitTableAndColumnName((yyvsp[(1) - (3)].str));
  }
    break;

  case 36:

/* Line 1806 of yacc.c  */
#line 1324 "TinySQL.y"
    {
      from_table_names.push_back((yyvsp[(1) - (1)].str));
    }
    break;

  case 37:

/* Line 1806 of yacc.c  */
#line 1327 "TinySQL.y"
    {
      from_table_names.push_back((yyvsp[(1) - (3)].str));
    }
    break;

  case 38:

/* Line 1806 of yacc.c  */
#line 1332 "TinySQL.y"
    {
      (yyval.number) = 0;
    }
    break;

  case 39:

/* Line 1806 of yacc.c  */
#line 1335 "TinySQL.y"
    {
      (yyval.number) = 1;
    }
    break;

  case 49:

/* Line 1806 of yacc.c  */
#line 1350 "TinySQL.y"
    {
      where_clause += "< ";
    }
    break;

  case 50:

/* Line 1806 of yacc.c  */
#line 1352 "TinySQL.y"
    {
      where_clause += "> ";
    }
    break;

  case 51:

/* Line 1806 of yacc.c  */
#line 1354 "TinySQL.y"
    {
      where_clause += "= ";
    }
    break;

  case 58:

/* Line 1806 of yacc.c  */
#line 1363 "TinySQL.y"
    {
      where_clause += string((yyvsp[(1) - (1)].str)) + " ";
    }
    break;

  case 59:

/* Line 1806 of yacc.c  */
#line 1366 "TinySQL.y"
    {
        where_clause += string((yyvsp[(1) - (1)].str)) + " ";
      }
    break;

  case 60:

/* Line 1806 of yacc.c  */
#line 1369 "TinySQL.y"
    {
        where_clause += string((yyvsp[(1) - (1)].str)) + " ";
      }
    break;

  case 61:

/* Line 1806 of yacc.c  */
#line 1372 "TinySQL.y"
    {
        where_clause += string((yyvsp[(1) - (1)].str)) + " ";
      }
    break;

  case 63:

/* Line 1806 of yacc.c  */
#line 1378 "TinySQL.y"
    {
      (yyval.number) = 0;
    }
    break;

  case 64:

/* Line 1806 of yacc.c  */
#line 1381 "TinySQL.y"
    {
      (yyval.number) = 1;
      order_by_column = string((yyvsp[(3) - (3)].str));
    }
    break;

  case 65:

/* Line 1806 of yacc.c  */
#line 1385 "TinySQL.y"
    {
      (yyval.number) = 1;
      order_by_column = string((yyvsp[(3) - (3)].str));
    }
    break;

  case 66:

/* Line 1806 of yacc.c  */
#line 1392 "TinySQL.y"
    {
      where_clause += "& ";
    }
    break;

  case 67:

/* Line 1806 of yacc.c  */
#line 1397 "TinySQL.y"
    { 
      where_clause += "| ";
    }
    break;

  case 68:

/* Line 1806 of yacc.c  */
#line 1402 "TinySQL.y"
    {
      where_clause += "! ";
    }
    break;

  case 69:

/* Line 1806 of yacc.c  */
#line 1407 "TinySQL.y"
    { 
      where_clause += "[ ";
    }
    break;

  case 70:

/* Line 1806 of yacc.c  */
#line 1412 "TinySQL.y"
    {
      where_clause += "] ";
    }
    break;

  case 71:

/* Line 1806 of yacc.c  */
#line 1417 "TinySQL.y"
    { 
      where_clause += "( ";
    }
    break;

  case 72:

/* Line 1806 of yacc.c  */
#line 1422 "TinySQL.y"
    {
      where_clause += ") ";
    }
    break;

  case 73:

/* Line 1806 of yacc.c  */
#line 1427 "TinySQL.y"
    {
      where_clause += "+ ";
    }
    break;

  case 74:

/* Line 1806 of yacc.c  */
#line 1432 "TinySQL.y"
    { 
      where_clause += "- ";
    }
    break;

  case 75:

/* Line 1806 of yacc.c  */
#line 1437 "TinySQL.y"
    {
      where_clause += "* ";
    }
    break;

  case 76:

/* Line 1806 of yacc.c  */
#line 1442 "TinySQL.y"
    {
      where_clause += "/ ";
    }
    break;



/* Line 1806 of yacc.c  */
#line 3148 "TinySQL.cc"
      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;


      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}



