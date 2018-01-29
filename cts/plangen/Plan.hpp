#ifndef H_cts_plangen_Plan
#define H_cts_plangen_Plan
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
#include "cts/infra/Pool.hpp"
#include "cts/infra/BitSet.hpp"
#include <iostream>

//---------------------------------------------------------------------------
/// A plan fragment
struct Plan
{
   /// Possible operators
   enum Op { IndexScan, AggregatedIndexScan, FullyAggregatedIndexScan, NestedLoopJoin, MergeJoin, HashJoin, HashGroupify, Filter, Union, MergeUnion, TableFunction, Singleton };
   /// Data Movement in distributed setting left2right: left node -> right node or right2left: right node ->left node or nomove: in node operation
   enum Transfer {nomove = 0, left2right, right2left};

   /// The cardinality type
   typedef double card_t;
   /// The cost type
   typedef double cost_t;

   /// The root operator
   Op op;
   /// transfer
   Transfer transfer;
   /// Node of Operator execution
   BitSet node_loc;
   /// Operator argument
   unsigned opArg;
   /// Its input
   Plan* left,*right;
   /// The resulting cardinality
   card_t cardinality;
   /// The resulting cardinality
   card_t est_cardinality;
   /// The resulting relation width
   BitSet width;
   /// The total costs
   cost_t costs;
   /// For Index scan
   unsigned sub;
   /// For Index scan
   unsigned pred;
   /// For Index scan
   unsigned obj;
   /// The ordering
   unsigned ordering;
   ///ID
   unsigned id;

   /// The next plan in problem chaining
   Plan* next;

   /// Print the plan
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
/// A container for plans. Encapsulates the memory management
class PlanContainer
{
   private:
   /// The pool
   StructPool<Plan> pool;

   public:
   /// Constructor
   PlanContainer();
   /// Destructor
   ~PlanContainer();

   /// Allocate a new plan
   Plan* alloc() { return pool.alloc(); }
   /// Release an allocate plan
   void free(Plan* p) { pool.free(p); }
   /// Release all plans
   void clear();
};
//---------------------------------------------------------------------------
#endif
