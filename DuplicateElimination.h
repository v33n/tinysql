#ifndef _DUPLICATEELIMINATION_H
#define _DUPLICATEELIMINATION_H

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

bool isSmaller(vector<string>, vector<enum FIELD_TYPE> , Tuple, Block *, int);

bool isEqual(vector<string>, vector<enum FIELD_TYPE>, Tuple, Tuple);

bool isTupleInMemory(Tuple, vector<string>, vector<string>);

void addTupleToMemory(Tuple);

void sortRelation(Relation *, Schema &, vector<string> &, vector<enum FIELD_TYPE> &);

Relation * removeDuplicatesTwoPass(Relation *, Schema &, vector<string> &, vector<enum FIELD_TYPE> &);

Relation * removeDuplicatesOnePass(Relation *, Schema &, vector<string> &, vector<enum FIELD_TYPE> &);

Relation * removeDuplicates(Relation *, vector<string> &, bool);

int getFirstMinTuple(vector<Tuple> &);

#endif
