#ifndef _JOIN_H
#define _JOIN_H

#include <string.h>

#include "Field.h"
#include "Tuple.h"

extern MainMemory mem;
extern Disk disk;
extern SchemaManager schema_manager;

void displayRelationInfo(Relation *);

bool isJoinTuple(Tuple t1, Tuple t2, vector<string> fields1, vector<string> fields2, vector<string> op);

int findSmallest(vector<Tuple> t1, vector<Tuple> t2, vector<string> fields1, vector<string> fields2, int &rel);

int findSmallestTuple(vector<Tuple>, vector<string>);

bool exhaustedAllSublists(vector<int> );

Relation *joinRelations(Relation *, Relation *, vector<string>, vector<string>, vector<string>, string rName = "J", string r1_name = "", string r2_name = "");

Relation *crossJoinRelations(Relation *r1, Relation *r2, string, vector<string> field1, vector<string> field2, vector<string> op);

Relation *fastCrossJoin(Relation *rptr1, Relation *rptr2, string, vector<string> fields1, vector<string> fields2, vector<string> op);

Relation *slowCrossJoin(Relation *rptr1, Relation *rptr2, string, vector<string> fields1, vector<string> fields2, vector<string> op);

Relation *naturalJoinRelations(Relation *rptr1, Relation *rptr2, string, vector<string> fields1, vector<string> fields2, vector<string> op, string, string);

Relation *onePassNaturalJoin(Relation *rptr1, Relation *rptr2, string, vector<string> fields1, vector<string> fields2, vector<string> op, string, string);

Relation *twoPassNaturalJoin(Relation *rptr1, Relation *rptr2, string, vector<string> fields1, vector<string> fields2, vector<string> op);
#endif
