#ifndef H_cts_plangen_PlanGen
#define H_cts_plangen_PlanGen
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
#include "cts/plangen/Plan.hpp"
#include "cts/infra/BitSet.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "Costs.hpp"
#include <util/evaluation.h>
#include <database/Database.hpp>




//---------------------------------------------------------------------------
/// A plan generator that construct a physical plan from a query graph
using namespace rg;
class PlanGen
{
   private:
   /// A subproblem
   struct Problem {
      /// The next problem in the DP table
      Problem* next;
      /// The known solutions to the problem
      Plan* plans;
      /// The relations involved in the problem
      BitSet relations;
      /// Variables of relation
      BitSet variables;
   };
   /// A join description
   struct JoinDescription;
   /// The plans
   PlanContainer plans;
   /// The problems
   StructPool<Problem> problems;
   /// The database
   Database *db;
   /// The current query
   const QueryGraph* fullQuery;

   PlanGen(const PlanGen&);

   void operator=(const PlanGen&);

   /// Add a plan to a subproblem
   void addPlan(Problem* problem,Plan* plan);
   /// Generate an index scan
   void buildIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* problem,BitSet &width,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C,unsigned value3,unsigned value3C);
   /// Generate an aggregated index scan
   void buildAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* problem,BitSet &width,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C);
   /// Generate an fully aggregated index scan
   void buildFullyAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,BitSet &width,unsigned value1,unsigned value1C);
   /// Generate base table accesses
   Problem* buildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id);
   /// Build the informaion about a join
   JoinDescription buildJoinInfo(const QueryGraph::SubQuery& query,const QueryGraph::Edge& edge);
   /// Generate an optional part
   Problem* buildOptional(const QueryGraph::SubQuery& query,unsigned id);
   /// Generate a union part
   Problem* buildUnion(const std::vector<QueryGraph::SubQuery>& query,unsigned id);
   /// Generate a table function access
   Problem* buildTableFunction(const QueryGraph::TableFunction& function,unsigned id);

   /// Translate a query into an operator tree
   Plan* translate(const QueryGraph::SubQuery& query);

   /// Pre-requisities
   void preBuildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id, vector<unsigned> & vect);

   public:
   /// Constructor
   PlanGen();
   /// Destructor
   ~PlanGen();

   /// Translate a query into an operator tree
   Plan* translate(Database* db,const QueryGraph& query);

   /// Pre-requisities
   void prerequistes(Database* db, const QueryGraph & query, vector<unsigned> & vect);

};
//---------------------------------------------------------------------------
#endif
