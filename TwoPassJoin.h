#ifndef _TESTJOIN_H
#define _TESTJOIN_H

#include <vector>
#include "Tuple.h"
#include "Block.h"
#include "Relation.h"
#include "Schema.h"
#include "Field.h"

extern MainMemory mem;
extern Disk disk;
extern SchemaManager schema_manager;

extern void appendTupleToRelation(Relation *, int, Tuple &);

extern void sortRelation(Relation *, Schema &, vector<string> &, vector<enum FIELD_TYPE> &);

extern bool isSmaller(vector<string>, vector<enum FIELD_TYPE> , Tuple, Block *, int);

extern bool isEqual(vector<string>, vector<enum FIELD_TYPE> , Tuple, Tuple);

extern int getFirstMinTuple(vector<Tuple> &);

Relation * sortJoinRelationsTwoPass(Relation *, Relation *, string, string, string, string);

#endif
