#include "Plan.hpp"
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
PlanContainer::PlanContainer()
   // Constructor
{
}
//---------------------------------------------------------------------------
PlanContainer::~PlanContainer()
   // Destructor
{
}
//---------------------------------------------------------------------------
void PlanContainer::clear()
   // Release all plans
{
   pool.freeAll();
}
//---------------------------------------------------------------------------
void Plan::print(unsigned indent)
   // Print the plan
{
   for (unsigned index=0;index<indent;index++)
      cout << ' ';
   switch (op) {
      case IndexScan: cout << "IndexScan"; break;
      case AggregatedIndexScan: cout << "AggregatedIndexScan"; break;
      case FullyAggregatedIndexScan: cout << "FullyAggregatedIndexScan"; break;
      case NestedLoopJoin: cout << "NestedLoopJoin"; break;
      case MergeJoin: cout << "MergeJoin"; break;
      case HashJoin: cout << "HashJoin"; break;
      case HashGroupify: cout << "HashGroupify"; break;
      case Filter: cout << "Filter"; break;
      case Union: cout << "Union"; break;
      case MergeUnion: cout << "MergeUnion"; break;
      case TableFunction: cout << "TableFunction"; break;
      case Singleton: cout << "Singleton"; break;
   }
   unsigned long loc = node_loc.valueOf();
   if(op == IndexScan || op == AggregatedIndexScan){
	   cout << " order= " << opArg << " cardinality=" << cardinality << " costs=" << costs << " Move:"<< transfer << " Node loc:" << loc  << " width: " << width.valueOf() << endl;
	   for (unsigned index=0;index<indent;index++) cout << ' ';
	   cout << " Sub=" << sub << " Pred=" << pred << " Obj:"<< obj << endl;
   }else
	   cout << " order= " << opArg << " cardinality=" << cardinality << " costs=" << costs << " Move:"<< transfer << " Node loc:" << loc  << " width: " << width.valueOf() << endl;

   switch (op) {
      case IndexScan: break;
      case AggregatedIndexScan: break;
      case FullyAggregatedIndexScan: break;
      case NestedLoopJoin:
      case MergeJoin:
      case HashJoin: left->print(indent+1); right->print(indent+1); break;
      case HashGroupify:
      case Filter: left->print(indent+1); break;
      case Union:
      case MergeUnion: left->print(indent+1); right->print(indent+1); break;
      case TableFunction: left->print(indent+1); break;
      case Singleton: break;
   }
}
//---------------------------------------------------------------------------
