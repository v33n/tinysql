#include <iostream>
#include <vector>
#include <sstream>

#include "Block.h"
#include "Config.h"
#include "Disk.h"
#include "Field.h"
#include "MainMemory.h"
#include "Relation.h"
#include "Schema.h"
#include "SchemaManager.h"
#include "Tuple.h"
#include "TwoPassJoin.h"

void getFieldNamesAndTypes(Relation *relation_ptr, vector<string> &join_field_names,
                           vector<enum FIELD_TYPE> &join_field_types, string common_field, 
                           string relation_name) {
  // string relation_name = relation_ptr->getRelationName();
  Schema schema = relation_ptr->getSchema();
  vector<string> field_names = schema.getFieldNames();
  vector<enum FIELD_TYPE> field_types = schema.getFieldTypes();

  for (int i = 0; i < field_names.size(); i++) {
    if (field_names[i] == common_field)
      continue;
    if (relation_name == "")
      join_field_names.push_back(field_names[i]);
    else
      join_field_names.push_back(relation_name + "." + field_names[i]);

    join_field_types.push_back(field_types[i]);
  }
}

void writeTuplesToJoin(Relation *join, Relation *R, vector<Tuple> tuples_R,
                       Relation *S, vector<Tuple> tuples_S, string common_field, 
                       string common_field_relation, string r1_name, string r2_name) {
  string relation_name, prefix;
  Schema schema;
  vector<string> field_names;
  for (int i = 0; i < tuples_R.size(); i++) {

    Tuple t = join->createTuple();

    // Set tuple with R fields
    //relation_name = R->getRelationName();
    relation_name = r1_name;
    schema = R->getSchema();
    field_names = (R->getSchema()).getFieldNames();
    if (relation_name == "")
      prefix = "";
    else
      prefix = relation_name + ".";

    for (int k = 0; k < field_names.size(); k++) {
      if (field_names[k] == common_field)
        continue;
      if (schema.getFieldType(field_names[k]) == INT)
	  t.setField(prefix + field_names[k], (tuples_R[i].getField(field_names[k])).integer);
      else
        t.setField(prefix + field_names[k], *((tuples_R[i].getField(field_names[k])).str));
    }

    // Set common field
      if (common_field_relation == "")
        prefix = "";
      else
        prefix = common_field_relation + ".";

      if (schema.getFieldType(common_field) == INT)
        t.setField(prefix + common_field, (tuples_R[i].getField(common_field)).integer);
      else
        t.setField(prefix + common_field, *((tuples_R[i].getField(common_field)).str));

    // Set tuple with S fields
    // relation_name = S->getRelationName();
    relation_name = r2_name;
    schema = S->getSchema();
    field_names = (S->getSchema()).getFieldNames();
    if (relation_name == "")
      prefix = "";
    else
      prefix = relation_name + ".";

    for (int j = 0; j < tuples_S.size(); j++) {
      for (int k = 0; k < field_names.size(); k++) {
        if (field_names[k] == common_field)
          continue;
      if (schema.getFieldType(field_names[k]) == INT)
        t.setField(prefix + field_names[k], (tuples_S[j].getField(field_names[k])).integer);
      else
        t.setField(prefix + field_names[k], *((tuples_S[j].getField(field_names[k])).str));
      }
      // Write this tuple
      appendTupleToRelation(join, 9, t);
    }
  }
}


vector<Tuple> getTuplesWithMinValue(Relation *relation_ptr, int start_block, int end_block, Tuple min_tuple,
                                    vector<int> &blocks_used, vector<string> field_name,
                                    vector<enum FIELD_TYPE> field_type, int num_groups1) {
  /* Get Tuples with min value for relation R */
  Block *block_ptr;
  vector<Tuple> tuples;
  bool readSublistBlock;

  int sorted_blocks = 9;
  int num_relation_blocks = relation_ptr->getNumOfBlocks();

  for (int i = start_block; i <= end_block; i++) {
    while (1) {
      block_ptr = mem.getBlock(i);
      vector<Tuple> t = block_ptr->getTuples();
      readSublistBlock = false;
      for(int j = 0; j < t.size(); j++) {
        if (t[j].isNull())
          continue;
        if (isEqual(field_name, field_type, min_tuple, t[j])) {
          tuples.push_back(t[j]);
          block_ptr->nullTuple(j);

          if (j == t.size() - 1) {
            // Get a new block from this sublist i in the memory
            blocks_used[i] += 1;
            if ((i == end_block) && (blocks_used[i] > (((num_relation_blocks - 1) % 9) + 1)))
              break;
            if (blocks_used[i] > sorted_blocks)
              break;
            // Bring another block from the sublist into the memory
            if (end_block == (num_groups1 - 1))
              relation_ptr->getBlock(i * sorted_blocks + blocks_used[i] - 1, i);
            else
              relation_ptr->getBlock((i - num_groups1) * sorted_blocks + blocks_used[i] - 1, i);
            readSublistBlock = true;
            break;
	  }
	}
      }
      if (!readSublistBlock)
        break;
    }
  }
  return tuples;
}

Relation * sortJoinRelationsTwoPass(Relation *relation_ptr1, Relation *relation_ptr2, string common_field,
                                    string common_field_relation, string r1_name, string r2_name) {
  // Every 9 blocks of the relation are sorted
  int sorted_blocks = 9, relation_blocks_index = 0;
  int num_relation_blocks1, num_relation_blocks2;
  int num_groups1, num_groups2, first_min_index = -1, i, j;
  Relation *join;
  Schema schema1, schema2;
  Block *block_ptr, *buf;
  vector<int> blocks_used;
  vector<string> field_name;
  vector<enum FIELD_TYPE> field_type;
  vector<Tuple> tuples;

  schema1 = relation_ptr1->getSchema();
  schema2 = relation_ptr2->getSchema();

  field_name.push_back(common_field);

  if (schema1.getFieldType(common_field) == INT)
    field_type.push_back(INT);
  else
    field_type.push_back(STR20);

  sortRelation(relation_ptr1, schema1, field_name, field_type);
  sortRelation(relation_ptr2, schema2, field_name, field_type);

  num_relation_blocks1 = relation_ptr1->getNumOfBlocks();
  num_relation_blocks2 = relation_ptr2->getNumOfBlocks();
  num_groups1 = (num_relation_blocks1 - 1) / 9 + 1;
  num_groups2 = (num_relation_blocks2 - 1) / 9 + 1;

  vector<string> join_field_names;
  vector<enum FIELD_TYPE> join_field_types;

  getFieldNamesAndTypes(relation_ptr1, join_field_names, join_field_types, common_field, r1_name);
  getFieldNamesAndTypes(relation_ptr2, join_field_names, join_field_types, common_field, r2_name);

  if (common_field_relation == "")
    join_field_names.push_back(common_field);
  else
    join_field_names.push_back(common_field_relation + "." + common_field);

  join_field_types.push_back((relation_ptr1->getSchema()).getFieldType(common_field));
  
  join = schema_manager.createRelation(
             relation_ptr1->getRelationName() + ".NaturalJoin." + relation_ptr2->getRelationName(),
             Schema(join_field_names, join_field_types));

  buf = mem.getBlock(9);

  // Bring first block of each group of first relation in memory
  for (i = 0; i < num_groups1; i++) {
    block_ptr = mem.getBlock(i);
    block_ptr->clear();
    relation_ptr1->getBlock(i * sorted_blocks, i);
    blocks_used.push_back(1);
  }

  // Bring first block of each group of second relation in memory
  // Can occupy only blocks upto index 8
  for (j = i; j < i + num_groups2; j++) {
    block_ptr = mem.getBlock(j);
    block_ptr->clear();
    relation_ptr2->getBlock((j-i) * sorted_blocks, j);
    blocks_used.push_back(1);
  }

  while(1) {
    Block *min_tuple_block_ptr = NULL;
    int min_tuple_index = -1, min_block_index = -1;

    for(int i = 0; i < num_groups1 + num_groups2; i++) {

      block_ptr = mem.getBlock(i);
      tuples = block_ptr->getTuples();

      // Get the first minimum unconsidered tuple overall from the sublist i
      while ( (first_min_index = getFirstMinTuple(tuples)) == -1) {
        // Use 1 more block
        blocks_used[i] += 1;

        if ( ((i == num_groups1 - 1) && (blocks_used[i] > (((num_relation_blocks1 - 1) % 9) + 1))) || 
             ((i == num_groups1 + num_groups2 - 1) && (blocks_used[i] > ((num_relation_blocks2 - 1) % 9 + 1))))
          break;

        if (blocks_used[i] > sorted_blocks)
          break;

        // Bring another block from the sublist into the memory
        if (i < num_groups1)
          relation_ptr1->getBlock(i * sorted_blocks + blocks_used[i] - 1, i);
        else
          relation_ptr2->getBlock((i - num_groups1) * sorted_blocks + blocks_used[i] - 1, i);

        tuples = block_ptr->getTuples();
      }

      // This sublist has been exhausted
      if (first_min_index == -1)
        continue;

      if (isSmaller(field_name, field_type, block_ptr->getTuple(first_min_index),
                    min_tuple_block_ptr, min_tuple_index)) {
        min_tuple_block_ptr = block_ptr;
        min_tuple_index = first_min_index;
        min_block_index = i;  
      }
    }

    if (!min_tuple_block_ptr)
      break;

    Tuple min_tuple = min_tuple_block_ptr->getTuple(min_tuple_index);
    // min_tuple_block_ptr->nullTuple(min_tuple_index);

    int R_start_block, R_end_block, S_start_block, S_end_block;
    Relation *R = relation_ptr1, *S = relation_ptr2;
    R_start_block = 0;
    R_end_block = num_groups1 - 1;
    S_start_block = num_groups1;
    S_end_block = num_groups1 + num_groups2 - 1;

    if (min_block_index >= num_groups1) {
      R = relation_ptr2;
      S = relation_ptr1;
      S_start_block = 0;
      S_end_block = num_groups1 - 1;
      R_start_block = num_groups1;
      R_end_block = num_groups1 + num_groups2 - 1;
    }

    vector<Tuple> tuples_R = getTuplesWithMinValue(R, R_start_block, R_end_block,
                                                   min_tuple, blocks_used, field_name,
                                                   field_type, num_groups1);
    vector<Tuple> tuples_S = getTuplesWithMinValue(S, S_start_block, S_end_block,
                                                   min_tuple, blocks_used, field_name,
                                                   field_type, num_groups1);

    if (!tuples_R.empty() && !tuples_S.empty())
      writeTuplesToJoin(join, R, tuples_R, S, tuples_S, common_field, common_field_relation, r1_name, r2_name);
  }

  return join;        
}
