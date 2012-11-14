#include <iostream>
#include <ctime>
#include <climits>
#include <string>
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
#include "test_join.h"
#include "DuplicateElimination.h"

int disp = 0;

//To check whether the tuple is valid for a Natural Join
bool isJoinTuple(Tuple t1, Tuple t2, vector<string> fields1, vector<string> fields2, vector<string> op)
{
	if(op.size() == 0)
		return true;
	else for(int i=0; i<op.size(); i++)
		{
		//cout<<"Operator: "<<op[i]<<" "<<fields2[i]<<endl;
		if(op[i] == "=")
		{
			if(t1.getSchema().getFieldType(fields1[i]) == INT && (t1.getField(fields1[i]).integer == t2.getField(fields2[i]).integer))
				continue;
			else if(t1.getSchema().getFieldType(fields1[i]) == STR20 && (*(t1.getField(fields1[i]).str) == *(t2.getField(fields2[i]).str)))
					continue;
			else return false;		
		} 
		else if(op[i] == ">")
			{
				if(t1.getSchema().getFieldType(fields1[i]) == INT && (t1.getField(fields1[i]).integer > t2.getField(fields2[i]).integer))
					continue;
				else return false;
			}
			else if(op[i] ==  "<")
			{
				if(t1.getSchema().getFieldType(fields1[i]) == INT && (t1.getField(fields1[i]).integer < t2.getField(fields2[0]).integer))
					continue;
				else return false;
			}
			else return false;
		}
	return true;	
}
int findSmallestTuple(vector<Tuple> t, vector<string> fields)
{
	int smallest = 0;
	int flag = 0;
	for(int j=1; j<t.size(); j++)
	{
		flag = 0;
		for(int i=0; i<fields.size(); i++)
		{
			if(t[j].getSchema().getFieldType(fields[i]) == INT && (t[j].getField(fields[i]).integer < t[smallest].getField(fields[i]).integer))
			{
				flag = 1;
				break;
			}	
			else if(t[j].getSchema().getFieldType(fields[i]) == INT && (t[j].getField(fields[i]).integer > t[smallest].getField(fields[i]).integer))
			{
				flag = 0;
				break;
			}
		}
		if(flag)
			smallest = j;
	}	
	return smallest;
}

int findSmallest(vector<Tuple> t1, vector<Tuple> t2, vector<string> fields1, vector<string> fields2, int &rel)
{
	int smallest_t1,smallest_t2, smallest;
	int flag = 0;
	smallest_t1 = findSmallestTuple(t1, fields1);
	smallest_t2 = findSmallestTuple(t2, fields2);
	
	smallest = smallest_t1;
	rel = 1;

	for(int i=0; i<fields1.size(); i++)
	{
		if(t1[smallest_t1].getSchema().getFieldType(fields1[i]) == INT && (t1[smallest_t1].getField(fields1[i]).integer < t2[smallest_t2].getField(fields1[i]).integer))
			break;
		else if(t1[smallest_t1].getSchema().getFieldType(fields1[i]) == INT && (t1[smallest_t1].getField(fields1[i]).integer > t2[smallest_t2].getField(fields1[i]).integer))
		{
			rel = 2;
			smallest = smallest_t2;
			break;
		}	
	}
	return smallest;	
}


bool exhaustedAllSublists(vector<int> block_list)
{
	for(int i = 0; i< block_list.size(); i++)
		if(block_list[i])
			return 0;
	return 1;
}

Relation* joinRelations(Relation *r1, Relation *r2, vector<string> field1, vector<string> field2, vector<string> op, string rName, string r1_name, string r2_name)
{	
	if(op.size() > 0)
	  return naturalJoinRelations(r1, r2, rName, field1, field2 , op, r1_name, r2_name);
	else return crossJoinRelations(r1, r2, rName, field1, field2, op);
}

Relation *naturalJoinRelations(Relation *rptr1, Relation *rptr2, string rName, vector<string> field1, vector<string> field2, vector<string> op, string r1_name, string r2_name)
{
  if(rptr2->getNumOfBlocks() < (mem.getMemorySize() - 2) || (rptr1->getNumOfBlocks() < mem.getMemorySize() - 2))
	  return onePassNaturalJoin(rptr1, rptr2, rName, field1, field2, op, r1_name, r2_name);
  else
    return sortJoinRelationsTwoPass(rptr1, rptr2, field1[0], r1_name, r1_name, r2_name);	
	  // return twoPassNaturalJoin(rptr1, rptr2, rName, field1, field2, op);

}

Relation *crossJoinRelations(Relation *r1, Relation *r2, string r3, vector<string> field1, vector<string> field2, vector<string> op)
{
	if(r2->getNumOfBlocks() > (mem.getMemorySize() - 2) || (r1->getNumOfBlocks() > mem.getMemorySize() - 2))
		return slowCrossJoin(r1, r2, r3, field1, field2, op);
	else	
		return fastCrossJoin(r1, r2, r3, field1, field2, op);
}


Relation *fastCrossJoin(Relation *rptr1, Relation *rptr2, string r3, vector<string> fields1, vector<string> fields2, vector<string> op)
{
	Schema s1, s2;
	vector <string> fields_r1, fields_r2, fields_r3;
	vector <enum FIELD_TYPE> types_r3;
		
	//Relation *rptr1, *rptr2;
	vector<string> values_r3;
	int num_blocks_r1, num_blocks_r2;
	num_blocks_r1 = rptr1->getNumOfBlocks();
	num_blocks_r2 = rptr2->getNumOfBlocks();

	s1 = rptr1->getSchema();
	s2 = rptr2->getSchema();
	fields_r1 = s1.getFieldNames();
	fields_r2 = s2.getFieldNames();
	
	if(disp)
	{
		cout<<"#"<<rptr1->getRelationName()<<" ="<<num_blocks_r1<<endl;
		cout<<"#"<<rptr2->getRelationName()<<" ="<<num_blocks_r2<<endl;
	}

	for(int i=0; i<fields_r1.size(); i++)
	{			
		fields_r3.push_back(rptr1->getRelationName() + "." + fields_r1[i]);
		types_r3.push_back(s1.getFieldType(fields_r1[i]));
	}	

	for(int i=0; i<fields_r2.size(); i++)
	{
		fields_r3.push_back(rptr2->getRelationName() + "." + fields_r2[i]);
		types_r3.push_back(s2.getFieldType(fields_r2[i]));
	}
	
	Schema s3(fields_r3,types_r3);
	//string r3 = rptr1->getRelationName() + "." + rptr2->getRelationName() + ".CrossJoin";
   	Relation *rptr3 = schema_manager.createRelation(r3, s3);

	if(disp)
	{
		cout<<"New Relation: "<<r3<<endl;
		cout<<s3<<endl;	
	}

	//Get tuples of smaller relation into Main Memory, assuming all can fit in
	if( num_blocks_r1 > num_blocks_r2 ) 
	{
		rptr2->getBlocks(0, 0, num_blocks_r2);
		vector<Tuple> tuples_r3, tuples_r2 = mem.getTuples(0, num_blocks_r2);
	
		//Read one block of relation into Main memory and combine each tuple in the block
		//with all the tuples in the other
		Block *block_r1 = mem.getBlock(num_blocks_r2);
		Block *block_r3 = mem.getBlock(num_blocks_r2 + 1);
   	   	block_r3->clear();    
		for(int i=0; i<num_blocks_r1; i++)
		{
   	  		block_r1->clear();    
		    rptr1->getBlock(i, num_blocks_r2);
   			vector<Tuple> tuples_r1 = block_r1->getTuples();

			for(int j =0; j < tuples_r1.size(); j++)
			{
				for(int k=0; k < tuples_r2.size(); k++)
				{
					Tuple tuple = rptr3->createTuple();
					if(isJoinTuple(tuples_r1[j], tuples_r2[k], fields1, fields2, op))
					{
						for(int l =0; l < fields_r1.size(); l++)
						{
							if(s3.getFieldType(rptr1->getRelationName() + "." + fields_r1[l]) == INT)
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],tuples_r1[j].getField(fields_r1[l]).integer);
							else
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],*(tuples_r1[j].getField(fields_r1[l]).str));
						}

						for(int l =0; l < fields_r2.size(); l++)
						{
							if(s3.getFieldType(rptr2->getRelationName() + "." + fields_r2[l]) == INT)
								tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],tuples_r2[k].getField(fields_r2[l]).integer);
							else
								tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],*(tuples_r2[k].getField(fields_r2[l]).str));
						}
						if(disp)
							cout<<"New Tuple:"<<tuple<<endl;
						tuples_r3.push_back(tuple);
						block_r3->appendTuple(tuple);
						//If main memory block is full, write it to disk and clear that block
						if(tuples_r3.size() == s3.getTuplesPerBlock())
						{
							rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r2 + 1);
								tuples_r3.clear();
							block_r3->clear();
						}
					}
				}
			}
		}
		//For the last tuple which might not be full, need to write that to disk too
		if(tuples_r3.size() !=0)
			rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r2 + 1);
		if(disp)
		{
			cout<<*rptr3<<endl;	

		}	
	} else
	{
		rptr1->getBlocks(0, 0, num_blocks_r1);
		vector<Tuple> tuples_r3, tuples_r1 = mem.getTuples(0, num_blocks_r1);
	
		//Read one block of rptr1 into Main memory and combine each tuple in the block
		//with all the tuples in rptr2
		Block *block_r2 = mem.getBlock(num_blocks_r1);
		Block *block_r3 = mem.getBlock(num_blocks_r1 + 1);
   	   	block_r3->clear();    
		for(int i=0; i<num_blocks_r2; i++)
		{
   	  		block_r2->clear();    
		    rptr2->getBlock(i, num_blocks_r1);
   			vector<Tuple> tuples_r2 = block_r2->getTuples();

			for(int j =0; j < tuples_r2.size(); j++)
			{
				for(int k=0; k < tuples_r1.size(); k++)
				{
					Tuple tuple = rptr3->createTuple();
					if(isJoinTuple(tuples_r1[k], tuples_r2[j], fields1, fields2, op))
					{
						for(int l =0; l < fields_r1.size(); l++)
						{
							if(s3.getFieldType(rptr1->getRelationName() + "." + fields_r1[l]) == INT)
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],tuples_r1[k].getField(fields_r1[l]).integer);
							else
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],*(tuples_r1[k].getField(fields_r1[l]).str));
						}

						for(int l =0; l < fields_r2.size(); l++)
						{
							if(s3.getFieldType(rptr2->getRelationName() + "." + fields_r2[l]) == INT)
								tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],tuples_r2[j].getField(fields_r2[l]).integer);
							else
								tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],*(tuples_r2[j].getField(fields_r2[l]).str));
						}
						tuples_r3.push_back(tuple);
						block_r3->appendTuple(tuple);
						if(disp)
							cout<<"New Tuple:"<<tuple<<endl;
						//If main memory block is full, write it to disk and clear that block
						if(tuples_r3.size() == s3.getTuplesPerBlock())
						{
							rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r1 + 1);
								tuples_r3.clear();
							block_r3->clear();
						}
					}	
				}
			}
		}
		//For the last tuple which might not be full, need to write that to disk too
		if(tuples_r3.size() !=0)
			rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r1 + 1);
		if(disp)
		{
			cout<<*rptr3<<endl;	
		}	
	}
	return rptr3;
}

Relation *slowCrossJoin(Relation *rptr1, Relation *rptr2, string r3, vector<string> fields1, vector<string> fields2, vector<string> op)
{
	Schema s1, s2;
	vector <string> fields_r1, fields_r2, fields_r3;
	vector <enum FIELD_TYPE> types_r3;
		
	//Relation *rptr1, *rptr2;
	vector<string> values_r3;
	int num_blocks_r1, num_blocks_r2, max_blocks;
	num_blocks_r1 = rptr1->getNumOfBlocks();
	num_blocks_r2 = rptr2->getNumOfBlocks();
	max_blocks = mem.getMemorySize();

	s1 = rptr1->getSchema();
	s2 = rptr2->getSchema();
	fields_r1 = s1.getFieldNames();
	fields_r2 = s2.getFieldNames();
	
	if(disp)
	{
		cout<<"#"<<rptr1->getRelationName()<<" ="<<num_blocks_r1<<endl;
		cout<<"#"<<rptr2->getRelationName()<<" ="<<num_blocks_r2<<endl;
	}

	for(int i=0; i<fields_r1.size(); i++)
	{			
		fields_r3.push_back(rptr1->getRelationName() + "." + fields_r1[i]);
		types_r3.push_back(s1.getFieldType(fields_r1[i]));
	}	

	for(int i=0; i<fields_r2.size(); i++)
	{
		fields_r3.push_back(rptr2->getRelationName() + "." + fields_r2[i]);
		types_r3.push_back(s2.getFieldType(fields_r2[i]));
	}
	
	Schema s3(fields_r3,types_r3);
	//string r3 = rptr1->getRelationName() + "." + rptr2->getRelationName() + ".CrossJoin";
   	Relation *rptr3 = schema_manager.createRelation(r3, s3);

	if(disp)
	{
		cout<<"New Relation: "<<r3<<endl;
		cout<<s3<<endl;	
	}
	for(int id = 0; id<num_blocks_r2; id += (max_blocks - 2))
	{
		//Get tuples of smaller relation into Main Memory, assuming all can fit in
		rptr2->getBlocks(id, 0, max_blocks - 2);
		vector<Tuple> tuples_r3, tuples_r2 = mem.getTuples(0, max_blocks - 2);
	
		//Read one block of relation into Main memory and combine each tuple in the block
		//with all the tuples in the other
		Block *block_r1 = mem.getBlock(max_blocks - 2);
		Block *block_r3 = mem.getBlock(max_blocks - 1);
   	   	block_r3->clear();    
		for(int i=0; i<num_blocks_r1; i++)
		{
   	  		block_r1->clear();    
		    rptr1->getBlock(i, max_blocks - 2);
   			vector<Tuple> tuples_r1 = block_r1->getTuples();

			for(int j =0; j < tuples_r1.size(); j++)
			{
				for(int k=0; k < tuples_r2.size(); k++)
				{
					Tuple tuple = rptr3->createTuple();
					if(isJoinTuple(tuples_r1[j], tuples_r2[k], fields1, fields2, op))
					{
						for(int l =0; l < fields_r1.size(); l++)
						{
							if(s3.getFieldType(rptr1->getRelationName() + "." + fields_r1[l]) == INT)
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],tuples_r1[j].getField(fields_r1[l]).integer);
							else
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],*(tuples_r1[j].getField(fields_r1[l]).str));
						}

						for(int l =0; l < fields_r2.size(); l++)
						{
							if(s3.getFieldType(rptr2->getRelationName() + "." + fields_r2[l]) == INT)
								tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],tuples_r2[k].getField(fields_r2[l]).integer);
							else
								tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],*(tuples_r2[k].getField(fields_r2[l]).str));		
						}
						if(disp)
							cout<<"New Tuple:"<<tuple<<endl;
						tuples_r3.push_back(tuple);
						block_r3->appendTuple(tuple);
						//If main memory block is full, write it to disk and clear that block
						if(tuples_r3.size() == s3.getTuplesPerBlock())
						{
							rptr3->setBlock(rptr3->getNumOfBlocks(),max_blocks - 1);
								tuples_r3.clear();
							block_r3->clear();
						}
					}
				}
			}
		}
		//For the last tuple which might not be full, need to write that to disk too
		if(tuples_r3.size() !=0)
			rptr3->setBlock(rptr3->getNumOfBlocks(),max_blocks - 1);
		if(disp)
		{
			cout<<*rptr3<<endl;
		}	
	}	
	return rptr3;	
} 

Relation *onePassNaturalJoin(Relation *rptr1, Relation *rptr2, string r3, vector<string> fields1, vector<string> fields2, vector<string> op, string r1_name, string r2_name)
{
	Schema s1, s2;
	bool flag = 1;
	vector <string> fields_r1, fields_r2, fields_r3;
	vector <enum FIELD_TYPE> types_r3;
	
	//Relation *rptr1, *rptr2;
	vector<string> values_r3;
	int num_blocks_r1, num_blocks_r2;
	num_blocks_r1 = rptr1->getNumOfBlocks();
	num_blocks_r2 = rptr2->getNumOfBlocks();

	s1 = rptr1->getSchema();
	s2 = rptr2->getSchema();
	fields_r1 = s1.getFieldNames();
	fields_r2 = s2.getFieldNames();

	for(int i=0; i<fields_r1.size(); i++){			
		fields_r3.push_back(r1_name + "." + fields_r1[i]);
		types_r3.push_back(s1.getFieldType(fields_r1[i]));
	}	

	//Find common attributes of both relations
	for(int i=0; i<fields_r2.size(); i++)
	{
		flag = 1;
		for(int k=0; k<op.size(); k++)
		{
			if(op[k] == "=" && fields_r2[i] == fields2[k] )
			{
				flag = 0;
				break;
			}
		}	
		if(flag)
		{
			fields_r3.push_back(r2_name + "." + fields_r2[i]);
			types_r3.push_back(s2.getFieldType(fields_r2[i]));
		}	
	}

	Schema s3(fields_r3,types_r3);
	//string r3 = rptr1->getRelationName() + "." + rptr2->getRelationName() + ".NaturalJoin";
	Relation *rptr3 = schema_manager.createRelation(r3, s3);

	if(disp)
	{
		displayRelationInfo(rptr3);
		cout<<s3<<endl;
	}

	//Get tuples of smaller relation into Main Memory, assuming all can fit in
	if( num_blocks_r1 > num_blocks_r2 ) 
	{
		rptr2->getBlocks(0, 0, num_blocks_r2);
		vector<Tuple> tuples_r3, tuples_r2 = mem.getTuples(0, num_blocks_r2);
	
		//Read one block of relation into Main memory and combine each tuple in the block
		//with all the tuples in the other
		Block *block_r1 = mem.getBlock(num_blocks_r2);
		Block *block_r3 = mem.getBlock(num_blocks_r2 + 1);
   	   	block_r3->clear();    
		for(int i=0; i<num_blocks_r1; i++)
		{				
	  		block_r1->clear();    
		    rptr1->getBlock(i, num_blocks_r2);
  			vector<Tuple> tuples_r1 = block_r1->getTuples();

			for(int j =0; j < tuples_r1.size(); j++)
			{
				for(int k=0; k < tuples_r2.size(); k++)
				{
					Tuple tuple = rptr3->createTuple();
					if(isJoinTuple(tuples_r1[j], tuples_r2[k], fields1, fields2, op))
					{
						for(int l =0; l < fields_r1.size(); l++)
						{
							if(s3.getFieldType(r1_name + "." + fields_r1[l]) == INT)
								tuple.setField(r1_name + "." + fields_r1[l],tuples_r1[j].getField(fields_r1[l]).integer);
							else
								tuple.setField(r1_name + "." + fields_r1[l],*(tuples_r1[j].getField(fields_r1[l]).str));
						}
	
						for(int l =0; l < fields_r2.size(); l++)
						{
							flag = 1;
							for(int m = 0; m < op.size(); m++)
							{
								if(op[m] == "=" && fields_r2[l].compare(fields2[m]) == 0)
								{
									flag = 0;
									break;
								}
							}	
							if(flag)
							{
								
								if(s3.getFieldType(r2_name + "." + fields_r2[l]) == INT)
									tuple.setField(r2_name + "." + fields_r2[l],tuples_r2[k].getField(fields_r2[l]).integer);
								else
									tuple.setField(r2_name + "." + fields_r2[l],*(tuples_r2[k].getField(fields_r2[l]).str));
							}
						}
						tuples_r3.push_back(tuple);
						block_r3->appendTuple(tuple);
						//If main memory block is full, write it to disk and clear that block
						if(tuples_r3.size() == s3.getTuplesPerBlock())
						{
							rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r2 + 1);
								tuples_r3.clear();
							block_r3->clear();
						}
					}
				}
			}
		}
		//For the last tuple which might not be full, need to write that to disk too
		if(tuples_r3.size() !=0)
			rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r2 + 1);
		if(disp)
		{
			cout<<*rptr3<<endl;	
		}
	} else
	{
		rptr1->getBlocks(0, 0, num_blocks_r1);
		vector<Tuple> tuples_r3, tuples_r1 = mem.getTuples(0, num_blocks_r1);
	
		//Read one block of rptr1 into Main memory and combine each tuple in the block
		//with all the tuples in rptr2
		Block *block_r2 = mem.getBlock(num_blocks_r1);
		Block *block_r3 = mem.getBlock(num_blocks_r1 + 1);
   	   	block_r3->clear();    
		for(int i=0; i<num_blocks_r2; i++)
		{
	  		block_r2->clear();    
		    rptr2->getBlock(i, num_blocks_r1);
  			vector<Tuple> tuples_r2 = block_r2->getTuples();

			for(int j =0; j < tuples_r2.size(); j++)
			{
				for(int k=0; k < tuples_r1.size(); k++)
				{
					Tuple tuple = rptr3->createTuple();
					if(isJoinTuple(tuples_r1[k], tuples_r2[j], fields1, fields2, op))
					{
						for(int l =0; l < fields_r1.size(); l++)
						{
							if(s3.getFieldType(r1_name + "." + fields_r1[l]) == INT)
								tuple.setField(r1_name + "." + fields_r1[l],tuples_r1[k].getField(fields_r1[l]).integer);
							else
								tuple.setField(r1_name + "." + fields_r1[l],*(tuples_r1[k].getField(fields_r1[l]).str));
						}

						for(int l =0; l < fields_r2.size(); l++)
						{	
							flag = 1;
							for(int m = 0; m < op.size(); m++)
							{								
								if(op[m] == "=" && fields_r2[l].compare(fields2[m]) == 0)
								{
									flag = 0;
									break;
								}
							}
							if(flag)
							{	
								if(s3.getFieldType(r2_name + "." + fields_r2[l]) == INT)
									tuple.setField(r2_name + "." + fields_r2[l],tuples_r2[j].getField(fields_r2[l]).integer);
								else
									tuple.setField(r2_name + "." + fields_r2[l],*(tuples_r2[j].getField(fields_r2[l]).str));
							}		
						}
						tuples_r3.push_back(tuple);
						block_r3->appendTuple(tuple);
						//If main memory block is full, write it to disk and clear that block
						if(tuples_r3.size() == s3.getTuplesPerBlock())
						{
							rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r1 + 1);
								tuples_r3.clear();
							block_r3->clear();
						}
					}
				}	
			}
		}
		//For the last tuple which might not be full, need to write that to disk too
		if(tuples_r3.size() !=0)
			rptr3->setBlock(rptr3->getNumOfBlocks(),num_blocks_r1 + 1);
		if(disp)
		{
			cout<<*rptr3<<endl;	
		}	
	}	
        cout << "Was here\n";
	return rptr3;
}

Relation *twoPassNaturalJoin(Relation *rptr1, Relation *rptr2, string r3, vector<string> fields1, vector<string> fields2, vector<string> op)
{
	int i1,j1,k,l,m,c1,c2;
	i1=j1=k=l=m=c1=c2 = 0;
	Schema s1, s2;
	bool flag = 1;
	vector <string> fields_r1, fields_r2, fields_r3;
	vector<enum FIELD_TYPE> field_type1, field_type2, types_r3;
	Block *block_ptr1, *block_ptr2;
	
	//Relation *rptr1, *rptr2;
	vector<string> values_r3;
	int num_blocks_r1, num_blocks_r2, max_blocks;
	num_blocks_r1 = rptr1->getNumOfBlocks();
	num_blocks_r2 = rptr2->getNumOfBlocks();
	max_blocks = mem.getMemorySize();

	s1 = rptr1->getSchema();
	s2 = rptr2->getSchema();
	fields_r1 = s1.getFieldNames();
	fields_r2 = s2.getFieldNames();

	for(int i=0; i< fields1.size(); i++)
		field_type1.push_back(s1.getFieldType(fields1[i]));

	//Create sorted sublists of size <M
	sortRelation(rptr1, s1, fields1, field_type1);
	
	for(int i=0; i< fields2.size(); i++)
		field_type2.push_back(s2.getFieldType(fields2[i]));
	
	//Create sorted sublists of size <M
	sortRelation(rptr2, s2, fields2, field_type2);


	for(int i=0; i<fields_r1.size(); i++){			
		fields_r3.push_back(rptr1->getRelationName() + "." + fields_r1[i]);
		types_r3.push_back(s1.getFieldType(fields_r1[i]));
	}

	//Find common attributes of both relations
	for(int i=0; i<fields_r2.size(); i++)
	{
		flag = 1;
		for(int k=0; k<op.size(); k++)
		{
			if(op[k] == "=" && fields_r2[i] == fields2[k] )
			{
				flag = 0;
				break;
			}
		}	
		if(flag)
		{
			fields_r3.push_back(rptr2->getRelationName() + "." + fields_r2[i]);
			types_r3.push_back(s2.getFieldType(fields_r2[i]));
		}	
	}

	Schema s3(fields_r3,types_r3);
	//string r3 = rptr1->getRelationName() + "." + rptr2->getRelationName() + ".NaturalJoin";
	Relation *rptr3 = schema_manager.createRelation(r3, s3);

	if(disp)
	{
		//displayRelationInfo(rptr3);
		//cout<<s3<<endl;
		cout<<*rptr1<<endl;
		cout<<*rptr2<<endl;
	}	

	int num_sublists_r1 = num_blocks_r1/(max_blocks - 1)+((num_blocks_r1%(max_blocks - 1) > 0)?1:0);
	int num_sublists_r2 = num_blocks_r2/(max_blocks - 1)+((num_blocks_r2%(max_blocks - 1) > 0)?1:0);
	vector<int> block_used_r1, block_used_r2, disk_block_index_r1, disk_block_index_r2;

	if(disp)
	{
		cout<<"number of sublists in r1: "<<num_sublists_r1<<endl;
		cout<<"number of sublists in r2: "<<num_sublists_r2<<endl;
	}	

	//Get one block from each sublist into main memory
	for(k = 0; k < num_sublists_r1; k++)
	{
		if(disp)
		{
			cout<<"k: "<<k<<endl;
			cout<<"disk block index: "<<k*(max_blocks-1)<<endl;
		}

		block_ptr1 = mem.getBlock(k);
		rptr1->getBlock(k*(max_blocks-1), k);
		disk_block_index_r1.push_back(1);
		block_used_r1.push_back(block_ptr1->getNumTuples());
		//block_ptr1->clear();
	}	
	for(l = 0; l < num_sublists_r2; l++)
	{
		if(disp)
		{
			cout<<"l: "<<l<<endl;
			cout<<"disk block index: "<<l*(max_blocks - 1)<<endl;
		}	
		block_ptr2 = mem.getBlock(l+num_sublists_r1);
		rptr2->getBlock(l*(max_blocks-1), l + num_sublists_r1);
		disk_block_index_r2.push_back(1);
		block_used_r2.push_back(block_ptr2->getNumTuples());
		//block_ptr2->clear();
	}

	if(disp)
	{
		cout<<"Block Used 1: "<<block_used_r1.size()<<endl;
		cout<<"Block Used 2: "<<block_used_r2.size()<<endl;
	}

	vector<Tuple> tuples_r3;
	
	//Read one block of relation into Main memory and combine each tuple in the block
	//with all the tuples in the other
	//Block *block_r1 = mem.getBlock(max_blocks - 2);
	Block *block_r3 = mem.getBlock(max_blocks - 1);
   	block_r3->clear();    

	i1 = j1 = 1;
	bool done = false;
	int block_id, tuple_offset;
	while(!done)
	{

		vector<Tuple> tuples_r1 = mem.getTuples(0, num_sublists_r1);
		vector<Tuple> tuples_r2 = mem.getTuples(num_sublists_r1, num_sublists_r2);

			Tuple tuple = rptr3->createTuple();
			int rel_no = 0;
			int id = findSmallest(tuples_r1, tuples_r2, fields1, fields2, rel_no);
			if(disp)
				cout<<"id: "<<id<<"rel: "<<rel_no<<endl;
			if(rel_no == 1)
			{
				if(disp)
					cout<<"Smallest Tuple: "<<tuples_r1[id]<<endl;
		

				for(m = 0; m < tuples_r2.size(); m++)
				{
					if(isJoinTuple(tuples_r1[id], tuples_r2[m], fields1, fields2, op))
					{
					if(disp)
					{
						cout<<"\n********************\nJoining:"<<endl;
						cout<<tuples_r1[id]<<endl;
						cout<<tuples_r2[m]<<endl;
					}
					for(int l =0; l < fields_r1.size(); l++)
						{
							if(s3.getFieldType(rptr1->getRelationName() + "." + fields_r1[l]) == INT)
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],tuples_r1[id].getField(fields_r1[l]).integer);
							else
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],*(tuples_r1[id].getField(fields_r1[l]).str));
						}
						for(int l =0; l < fields_r2.size(); l++)
						{	
							int flag = 1;
							for(int n = 0; n < op.size(); n++)
							{								
								if(op[n] == "=" && fields_r2[l].compare(fields2[n]) == 0)
								{
									flag = 0;
									break;
								}
							}
							if(flag)
							{
								if(s3.getFieldType(rptr2->getRelationName() + "." + fields_r2[l]) == INT)
									tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],tuples_r2[m].getField(fields_r2[l]).integer);
								else
									tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],*(tuples_r2[m].getField(fields_r2[l]).str));
							}	
						}

						if(disp)
							cout<<"New Tuple:"<<tuple<<endl;

						tuples_r3.push_back(tuple);
						block_r3->appendTuple(tuple);
						//If main memory block is full, write it to disk and clear that block
						if(tuples_r3.size() == s3.getTuplesPerBlock())
						{
							rptr3->setBlock(rptr3->getNumOfBlocks(),max_blocks - 1);
								tuples_r3.clear();
							block_r3->clear();
						}
					}
				}

				//block_ptr1->clear();				
				if(s1.getTuplesPerBlock() == 1)
				{
					block_id = id;
					tuple_offset = 0;
				}	
				else	
				{
					block_id = id/num_sublists_r1;
					tuple_offset = id%num_sublists_r1;
				}	
				
				block_used_r1[block_id]--;
				block_ptr1 = mem.getBlock(block_id);
				block_ptr1->nullTuple(tuple_offset);
				//tuples_r1[id].null();

				//If a particular block of tuples has been used in the main memory, replenish from disk
				if(block_used_r1[block_id] == 0)
				{
					if(disp)
						cout<<"Block list consumed: "<<block_id<<endl;
						
					//If we have used up all the blocks in this sublist
					if(exhaustedAllSublists(block_used_r1))
						done = true;
					else
					{					
						cout<<disk_block_index_r1[block_id]<<endl;
						if(disk_block_index_r1[block_id] == ((max_blocks-1 < num_blocks_r1) ? (max_blocks - 1) : num_blocks_r1))
						{
							continue;
						}	
						else
						{
							block_ptr1->clear();
							block_ptr1 = mem.getBlock(block_id);
							rptr1->getBlock(disk_block_index_r1[block_id] + (block_id)*(max_blocks-1), block_id);
							block_used_r1[block_id] = block_ptr1->getNumTuples();
							++disk_block_index_r1[block_id];
						}	
					}	
				}
			}
			else if(rel_no == 2)
			{	
				if(disp)
					cout<<"Smallest Tuple: "<<tuples_r2[id]<<endl;

				for(m = 0; m < tuples_r1.size(); m++)
				{
					if(isJoinTuple(tuples_r1[m], tuples_r2[id], fields1, fields2, op))
					{
					
					if(disp)
					{
						cout<<"\n********************\nJoining:"<<endl;
						cout<<tuples_r1[m]<<endl;
						cout<<tuples_r2[id]<<endl;
					}
						for(int l =0; l < fields_r1.size(); l++)
						{
							if(s3.getFieldType(rptr1->getRelationName() + "." + fields_r1[l]) == INT)
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],tuples_r1[m].getField(fields_r1[l]).integer);
							else
								tuple.setField(rptr1->getRelationName() + "." + fields_r1[l],*(tuples_r1[m].getField(fields_r1[l]).str));
						}
						for(int l =0; l < fields_r2.size(); l++)
						{	
							int flag = 1;
							for(int n = 0; n < op.size(); n++)
							{								
								if(op[n] == "=" && fields_r2[l].compare(fields2[n]) == 0)
								{
									flag = 0;
									break;
								}
							}
							if(flag)
							{	
								if(s3.getFieldType(rptr2->getRelationName() + "." + fields_r2[l]) == INT)
									tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],tuples_r2[id].getField(fields_r2[l]).integer);
								else
									tuple.setField(rptr2->getRelationName() + "." + fields_r2[l],*(tuples_r2[id].getField(fields_r2[l]).str));
							}		
						}

						if(disp)
							cout<<"New Tuple:"<<tuple<<endl;

						tuples_r3.push_back(tuple);
						block_r3->appendTuple(tuple);
						//If main memory block is full, write it to disk and clear that block
						if(tuples_r3.size() == s3.getTuplesPerBlock())
						{
							rptr3->setBlock(rptr3->getNumOfBlocks(),max_blocks - 1);
								tuples_r3.clear();
							block_r3->clear();
						}
					}
				}
				
				//block_ptr2->clear();

				if(s2.getTuplesPerBlock() == 1)
				{
					block_id = id;
					tuple_offset = 0;
				}	
				else	
				{
					block_id = id/num_sublists_r2;
					tuple_offset = id%num_sublists_r2;
				}	
				
				block_used_r2[block_id]--;				
				block_ptr2 = mem.getBlock(num_sublists_r1 + block_id);
				block_ptr2->nullTuple(tuple_offset);

				//If a particular block of tuples has been used in the main memory, replenish from disk
				if(block_used_r2[block_id] == 0)
				{
					if(disp)
						cout<<"Block list consumed: "<<block_id<<endl;
						
					//If we have used up all the blocks in this sublist
					if(exhaustedAllSublists(block_used_r2))
						done = true;
					else
					{	
						cout<<disk_block_index_r2[block_id]<<endl;
						if(disk_block_index_r2[block_id] == ((max_blocks-1<< num_blocks_r2) ? (max_blocks - 1) : num_blocks_r2))
						{
							continue;
						}	
						else
						{
							block_ptr2->clear();
							block_ptr2 = mem.getBlock(num_sublists_r1 + block_id);
							rptr2->getBlock(disk_block_index_r2[block_id] + (block_id)*(max_blocks-1), num_sublists_r1 + block_id);
							block_used_r2[block_id] = block_ptr2->getNumTuples();
							++disk_block_index_r2[block_id];
						}	
					}	
				}
			}
			if(disp)
			{
				cout<<"#r3: "<<rptr3->getNumOfTuples()<<endl;
				//cout<<*rptr3<<endl;
				//cin.get();
			}	
	}
	//For the last tuple which might not be full, need to write that to disk too
	if(tuples_r3.size() !=0)
		rptr3->setBlock(rptr3->getNumOfBlocks(),max_blocks - 1);

	if(disp)
	{
		cout<<*rptr3<<endl;	
	}
	return rptr3;
}
