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
#include "OrderBy.h"


Relation * orderByRelation(Relation *relation_ptr, string name, enum FIELD_TYPE type,
                           bool returnRelation = false) {
  int mem_blocks = 10;
  Schema schema;
  vector<string> field_name;
  vector<enum FIELD_TYPE> field_type;
  
  schema = relation_ptr->getSchema();
  field_name.push_back(name);
  field_type.push_back(type);

  sortRelation(relation_ptr, schema, field_name, field_type);

  if (relation_ptr->getNumOfBlocks() < mem_blocks)
    return relation_ptr;
  cout << "\nUsing Two Pass sorting for ORDER BY clause \n";
  if (!returnRelation)
    displayRelationFields(relation_ptr);
  orderByRelationTwoPass(relation_ptr, field_name, field_type, returnRelation);
}

Relation * orderByRelationTwoPass(Relation *relation_ptr, vector<string> &field_name, vector<enum FIELD_TYPE> &field_type,
                            bool returnRelation) {
  Block *block_ptr;

  // Every 9 blocks of the relation are sorted
  int sorted_blocks = 9;
  int num_relation_blocks = relation_ptr->getNumOfBlocks();
  int num_groups = (num_relation_blocks - 1) / 9 + 1;
  int first_min_index;
  vector<int> blocks_used;
  Relation *ordered_relation_ptr = NULL;
  Schema schema;

  if (returnRelation) {
    schema = relation_ptr->getSchema();
    ordered_relation_ptr = schema_manager.createRelation(relation_ptr->getRelationName() + "_ordered",
						         Schema(schema.getFieldNames(), schema.getFieldTypes()));
  }

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
    if (!min_tuple_block_ptr)
      break;

    Tuple min_tuple = min_tuple_block_ptr->getTuple(min_tuple_index);
    if (!returnRelation)
      min_tuple.printTuple();
    else
      appendTupleToRelation(ordered_relation_ptr, 9, min_tuple);
    min_tuple_block_ptr->nullTuple(min_tuple_index);
  }
  return ordered_relation_ptr;
}
