#ifndef _ORDERBY_H
#define _ORDERBY_H

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

extern void displayRelationFields(Relation *);

extern void sortRelation(Relation *, Schema &, vector<string> &, vector<enum FIELD_TYPE> &);

extern bool isSmaller(vector<string>, vector<enum FIELD_TYPE> , Tuple, Block *, int);

extern int getFirstMinTuple(vector<Tuple> &);

Relation * orderByRelation(Relation *, string, enum FIELD_TYPE, bool);

Relation * orderByRelationTwoPass(Relation *, vector<string> &, vector<enum FIELD_TYPE> &, bool);

#endif
