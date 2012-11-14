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
#include "DuplicateElimination.h"

bool isSmaller(vector<string> field_name, vector<enum FIELD_TYPE> field_type, Tuple a, Block *ptr, int index) {
  int val1, val2;
  string str1, str2;

  if(!ptr)
    return true;

  Tuple b = ptr->getTuple(index);
  for (int i = 0; i < field_name.size(); i++) {
    if (field_type[i] == INT) {
      val1 = a.getField(field_name[i]).integer;
      val2 = b.getField(field_name[i]).integer; 
      if (val1 < val2)
        return true;
      else if(val1 > val2)
        return false;
    }
    else {
      str1 = *(a.getField(field_name[i]).str);
      str2 = *(b.getField(field_name[i]).str);
      if (str1 < str2)
        return true;
      else if(str1 > str2)
        return false;
    }
  }
  return false;
}

bool isEqual(vector<string> field_name, vector<enum FIELD_TYPE> field_type, Tuple min_tuple, Tuple last_dedup_tuple) {
  if (last_dedup_tuple.isNull())
    return false;
  stringstream s1, s2;
  for (int i = 0; i < field_name.size(); i++) {
    if (field_type[i] == INT) {
      s1 << min_tuple.getField(field_name[i]).integer;
      s2 << last_dedup_tuple.getField(field_name[i]).integer;
    } 
    else {
      s1 << *(min_tuple.getField(field_name[i]).str);
      s2 << *(last_dedup_tuple.getField(field_name[i]).str);
    }
    if (s1.str() != s2.str())
      return false;
    else
      continue;
  }
  return true;
}

void sortRelation(Relation *relation_ptr, Schema &schema,
                  vector<string> &field_name, vector<enum FIELD_TYPE> &field_type) {
  int mem_blocks = 9; // There are 9 memory blocks used for sorting
  Block *block_ptr;
  Block *buf; 
  int num_relation_blocks, relation_blocks_index = 0, num_blocks_processed = 0, cur_blocks;
  bool isLastBlockEmpty;

  buf = mem.getBlock(9);
  buf->clear();
  num_relation_blocks = relation_ptr->getNumOfBlocks();

  while(num_blocks_processed != num_relation_blocks) {
    
    cur_blocks = 9;
    if ((num_relation_blocks - num_blocks_processed) < 9) 
      cur_blocks = num_relation_blocks - num_blocks_processed;

    for(int i = 0; i < cur_blocks; i++) {
      block_ptr = mem.getBlock(i);
      block_ptr->clear();
      relation_ptr->getBlock(num_blocks_processed + i, i);
    }

    while(1) {

      Block *min_tuple_block_ptr = NULL;
      int min_tuple_index = -1;

      // Find out the min tuple from 0 - 8 blocks
      for(int i = 0; i < cur_blocks; i++) {
        block_ptr = mem.getBlock(i);
        vector<Tuple> tuples = block_ptr->getTuples();
        for(int j = 0; j < tuples.size(); j++) {
          if(tuples[j].isNull())
            continue;
          if(isSmaller(field_name, field_type, tuples[j], min_tuple_block_ptr, min_tuple_index)) {
            min_tuple_block_ptr = block_ptr;
            min_tuple_index = j;
	  }
        }
      }

      // Only if no min tuple found, get the next set of 9 blocks
      if (!min_tuple_block_ptr)
        break;

      // Copy the min tuple to relation
      buf->appendTuple(min_tuple_block_ptr->getTuple(min_tuple_index));
      min_tuple_block_ptr->nullTuple(min_tuple_index);
      if (buf->isFull() == 1) {
        relation_ptr->setBlock(relation_blocks_index, 9);
        buf->clear();
        relation_blocks_index++;
        isLastBlockEmpty = true;
      }
      else isLastBlockEmpty = false;
    } // while(1) loop

    num_blocks_processed += cur_blocks; 
  }
  
  if (!buf->isEmpty()) {
    relation_ptr->setBlock(relation_blocks_index, 9);
    buf->clear();
    relation_blocks_index++;
    isLastBlockEmpty = true;
  }

  if (!isLastBlockEmpty)
    relation_blocks_index++;
  
  if (relation_blocks_index <  num_relation_blocks - 1)
    relation_ptr->deleteBlocks(relation_blocks_index);
}

int getFirstMinTuple(vector<Tuple> &tuples) {
  for(int i = 0; i < tuples.size(); i++) {
    if (tuples[i].isNull())
      continue;
    return i;
  }
  return -1;
}

Relation * removeDuplicatesTwoPass(Relation *relation_ptr, Schema &schema,
                                   vector<string> &field_name, vector<enum FIELD_TYPE> &field_type) {
  Relation *dedup_relation_ptr;
  Block *block_ptr, *buf;
  // Every 9 blocks of the relation are sorted
  int sorted_blocks = 9, relation_blocks_index = 0;
  int num_relation_blocks = relation_ptr->getNumOfBlocks();
  int num_groups = (num_relation_blocks - 1) / 9 + 1;
  int first_min_index;
  vector<int> blocks_used;

  vector<string> field_names = schema.getFieldNames();
  vector<enum FIELD_TYPE> field_types = schema.getFieldTypes();
  Tuple last_dedup_tuple = relation_ptr->createTuple();
  last_dedup_tuple.null();

  dedup_relation_ptr = schema_manager.createRelation(relation_ptr->getRelationName() + "_dedup",
						     Schema(field_names, field_types));

  buf = mem.getBlock(9);
  // Bring first block of each group in memory
  for (int i = 0; i < num_groups; i++) {
    block_ptr = mem.getBlock(i);
    block_ptr->clear();
    relation_ptr->getBlock(i * sorted_blocks, i);
    blocks_used.push_back(1);
  }

  while(1) {
    Block *min_tuple_block_ptr = NULL;
    int min_tuple_index = -1;

    for(int i = 0; i < num_groups; i++) {

      block_ptr = mem.getBlock(i);
      vector<Tuple> tuples = block_ptr->getTuples();
      
      // Try to get the first unconsidered tuple from this sublist
      while ( (first_min_index = getFirstMinTuple(tuples)) == -1) {
        // Use 1 more block
        blocks_used[i] += 1;
        if ( (i == num_groups - 1) && (blocks_used[i] > ((num_relation_blocks - 1) % 9 + 1)))
          break;
        if (blocks_used[i] > sorted_blocks)
          break;
        // Bring another block from the sublist into the memory
        relation_ptr->getBlock(i * sorted_blocks + blocks_used[i] - 1, i);
        tuples = block_ptr->getTuples();
      }

      // This sublist has been exhausted
      if (first_min_index == -1)
        continue;

      if (isSmaller(field_name, field_type, block_ptr->getTuple(first_min_index),
                    min_tuple_block_ptr, min_tuple_index)) {
          min_tuple_block_ptr = block_ptr;
          min_tuple_index = first_min_index;
      }
    }

    // If no min tuple found, break
    if (!min_tuple_block_ptr || min_tuple_index == -1)
      break;

    Tuple min_tuple = min_tuple_block_ptr->getTuple(min_tuple_index);
    min_tuple_block_ptr->nullTuple(min_tuple_index);

    // Copy the min tuple to free block
    if (isEqual(field_name, field_type, min_tuple, last_dedup_tuple))
      continue;
    last_dedup_tuple = min_tuple;
    buf->appendTuple(min_tuple);
    if (buf->isFull() == 1) {
      dedup_relation_ptr->setBlock(relation_blocks_index, 9);
      buf->clear();
      relation_blocks_index++;
    }
  }

  if (!buf->isEmpty()) {
    dedup_relation_ptr->setBlock(relation_blocks_index, 9);
    buf->clear();
    relation_blocks_index++;
  }

  return dedup_relation_ptr;
}

bool isTupleInMemory(Tuple t, vector<string> field_name, vector<enum FIELD_TYPE> field_type) {
  Block *buf;
  int num_mem_blocks = 10;

  for(int i = 0; i < num_mem_blocks - 1; i++) {
    buf = mem.getBlock(i);
    vector<Tuple> tuples = buf->getTuples();
    for(int j = 0; j < tuples.size(); j++) {
      if (tuples[j].isNull())
        continue;
      if (isEqual(field_name, field_type, t, tuples[j]))
        return true;
    }
  }
  return false;
}

void addTupleToMemory(Tuple t) {
  Block *buf;
  int num_mem_blocks = 10;

  for(int i = 0; i < num_mem_blocks - 1; i++) {
    buf = mem.getBlock(i);
    if (buf->isFull() == 1)
      continue;
    buf->appendTuple(t);
    break;    
  }
}

Relation * removeDuplicatesOnePass(Relation *relation_ptr, Schema &schema,
                                   vector<string> &field_name, vector<enum FIELD_TYPE> &field_type) {
  Relation *dedup_relation_ptr;
  Block *block_ptr;
  int num_mem_blocks = 10, num_relation_blocks;

  vector<string> field_names = schema.getFieldNames();
  vector<enum FIELD_TYPE> field_types = schema.getFieldTypes();

  dedup_relation_ptr = schema_manager.createRelation(relation_ptr->getRelationName() + "_dedup",
						     Schema(field_names, field_types));

  num_relation_blocks = relation_ptr->getNumOfBlocks();

  // Clear all mem blocks
  for(int i = 0; i < num_mem_blocks; i++) {
    block_ptr = mem.getBlock(i);
    block_ptr->clear();
  }

  block_ptr = mem.getBlock(9);
  for (int i = 0; i < num_relation_blocks; i++) {  
    block_ptr->clear();
    relation_ptr->getBlock(i, 9);
    vector<Tuple> tuples = block_ptr->getTuples();
    for (int j = 0; j < tuples.size(); j++) {
      if (tuples[j].isNull() || isTupleInMemory(tuples[j], field_name, field_type))
        continue;
      // Add tuple to memory and relation
      addTupleToMemory(tuples[j]);
      appendTupleToRelation(dedup_relation_ptr, 9, tuples[j]);
    }
  }
  return dedup_relation_ptr;
}


Relation * removeDuplicates(Relation *relation_ptr, vector<string> &field_names, bool isAllDistinct) {
  int num_blocks, num_tuples, max_tuples_per_block;
  vector<string> field_name;
  vector<enum FIELD_TYPE> field_type;
  Schema schema;

  if (isAllDistinct)
    field_name = field_names;
  else
    field_name.push_back(field_names[0]);

  schema = relation_ptr->getSchema();

  num_blocks = relation_ptr->getNumOfBlocks();
  num_tuples = relation_ptr->getNumOfTuples();
  max_tuples_per_block = schema.getTuplesPerBlock();

  for (int i = 0; i < field_name.size(); i++)
    field_type.push_back(schema.getFieldType(field_name[i]));

  // Can use 9 blocks of memory for one pass
  if (num_tuples <= 9 * max_tuples_per_block) {
    // One pass algo
    cout << "\nUsing One pass Duplicate Elimination: \n"; 
    return removeDuplicatesOnePass(relation_ptr, schema, field_name, field_type);
  }
  else {
    cout << "Using Two pass Duplicate Elimination: \n";
    sortRelation(relation_ptr, schema, field_name, field_type);
    return removeDuplicatesTwoPass(relation_ptr, schema, field_name, field_type);
  }
}
