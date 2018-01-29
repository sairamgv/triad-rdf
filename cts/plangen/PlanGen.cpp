#include "PlanGen.hpp"
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
// XXX integrate DPhyper-based optimization, query path statistics
//---------------------------------------------------------------------------
/// Description for a join
struct PlanGen::JoinDescription
{
   /// Sides of the join
   BitSet left,right;
   /// Required ordering
   unsigned ordering;
   /// Selectivity
   double selectivity;
   /// Table function
   const QueryGraph::TableFunction* tableFunction;
};
//---------------------------------------------------------------------------
PlanGen::PlanGen()
   // Constructor
{
}
//---------------------------------------------------------------------------
PlanGen::~PlanGen()
   // Destructor
{
}

//---------------------------------------------------------------------------
void PlanGen::addPlan(Problem* problem,Plan* plan)
   // Add a plan to a subproblem
{
   // Check for dominance
   if (~plan->ordering) {
      Plan* last=0;
      for (Plan* iter=problem->plans,*next;iter;iter=next) {
         next=iter->next;
         if (iter->ordering==plan->ordering) {
            // Dominated by existing plan?
            if (iter->costs<=plan->costs) {
               plans.free(plan);
               return;
            }
            // No, remove the existing plan
            if (last)
               last->next=iter->next; else
               problem->plans=iter->next;
            plans.free(iter);
         } else if ((!~iter->ordering)&&(iter->costs>=plan->costs)) {
            // Dominated by new plan
            if (last)
               last->next=iter->next; else
               problem->plans=iter->next;
            plans.free(iter);
         } else last=iter;
      }
   } else {
      Plan* last=0;
      for (Plan* iter=problem->plans,*next;iter;iter=next) {
         next=iter->next;
         // Dominated by existing plan?
         if (iter->costs<=plan->costs) {
            plans.free(plan);
            return;
         }
         // Dominates existing plan?
         if (iter->ordering==plan->ordering) {
            if (last)
               last->next=iter->next; else
               problem->plans=iter->next;
            plans.free(iter);
         } else last=iter;
      }
   }

   // Add the plan to the problem set
   plan->next=problem->plans;
   problem->plans=plan;
}
//---------------------------------------------------------------------------
static Plan* buildFilters(PlanContainer& plans,const QueryGraph::SubQuery& query,Plan* plan,unsigned value1,unsigned value2,unsigned value3)
   // Apply filters to index scans
{
   // Collect variables
   set<unsigned> orderingOnly,allAttributes;
   if (~(plan->ordering))
      orderingOnly.insert(plan->ordering);
   allAttributes.insert(value1);
   allAttributes.insert(value2);
   allAttributes.insert(value3);

   // Apply a filter on the ordering first
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if ((*iter).isApplicable(orderingOnly)) {
         Plan* p2=plans.alloc();
         double cost1=plan->costs+Costs::filter(plan->cardinality);
         p2->op=Plan::Filter;
         p2->costs=cost1;
         p2->opArg=(*iter).id;
         p2->left=plan;
         p2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
         p2->next=0;
         p2->cardinality=plan->cardinality*0.5;
         p2->ordering=plan->ordering;
         plan=p2;
      }
   // Apply all other applicable filters
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if ((*iter).isApplicable(allAttributes)&&(!(*iter).isApplicable(orderingOnly))) {
         Plan* p2=plans.alloc();
         p2->op=Plan::Filter;
         p2->opArg=(*iter).id;
         p2->left=plan;
         p2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
         p2->next=0;
         p2->cardinality=plan->cardinality*0.5;
         p2->costs=plan->costs+Costs::filter(plan->cardinality);
         p2->ordering=plan->ordering;
         plan=p2;
      }
   return plan;
}
//---------------------------------------------------------------------------
static void normalizePattern(Database::DataOrder order,unsigned& c1,unsigned& c2,unsigned& c3)
    // Extract subject/predicate/object order
{
   unsigned s=~0u,p=~0u,o=~0u;
   switch (order) {
      case Database::Order_Subject_Predicate_Object: s=c1; p=c2; o=c3; break;
      case Database::Order_Subject_Object_Predicate: s=c1; o=c2; p=c3; break;
      case Database::Order_Object_Predicate_Subject: o=c1; p=c2; s=c3; break;
      case Database::Order_Object_Subject_Predicate: o=c1; s=c2; p=c3; break;
      case Database::Order_Predicate_Subject_Object: p=c1; s=c2; o=c3; break;
      case Database::Order_Predicate_Object_Subject: p=c1; o=c2; s=c3; break;
   }
   c1=s; c2=p; c3=o;
}
//---------------------------------------------------------------------------
static unsigned getCardinality(Database& db,Database::DataOrder order,unsigned v1,unsigned c1,unsigned v2, unsigned c2,unsigned v3,unsigned c3, unsigned &card, unsigned &org_card)
   // Estimate the cardinality of a predicate
{
   //normalizePattern(order,c1,c2,c3);
   //normalizePattern(order,v1,v2,v3);
   db.statistics->getCardinality(v1,c1,v2,c2,v3,c3, card, org_card, order);
   //std::cout << order << "::" << v1 << " " << c1 << "--" << v2 << " " << c2 << "--"<< v3 << " " << c3 << " " << card << endl;
   //std::cout << "#############################################"<< endl; //DEBUG
   return org_card;
}


static unsigned getLocation(Database& db,Database::DataOrder order,unsigned c1,unsigned c2,unsigned c3, bool summary)
   // Get the location of the triple
{
	if(summary)
		return ~0u;

   normalizePattern(order,c1,c2,c3);
   unsigned key;
   if(db.clusterSize < 2) return db.clusterSize;


   if(~c1)
		key = c1;
	else if(~c3)
		key = c3;
	else
		return ~0u;
 
   return (key >> db.get_pbits())%db.clusterSize + 1;
}

//---------------------------------------------------------------------------
void PlanGen::buildIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,BitSet &width,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C,unsigned value3,unsigned value3C)
   // Build an index scan
{
	//std::cout << "Index Scan" << endl;


	// Initialize a new plan
   Plan* plan=plans.alloc();
   plan->op=Plan::IndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;
   plan->node_loc = BitSet();
   plan->width = BitSet(width.valueOf());
   // Compute the statistics
   unsigned scanned;

   unsigned card, org_card;
   getCardinality(*db,order,value1, value1C, value2, value2C, value3, value3C, card, org_card);

   plan->cardinality=scanned=card;
   plan->est_cardinality =card;
   plan->node_loc.set(getLocation(*db,order,value1C, value2C,value3C,db->getEnv()));
   if(!~value1C)
	   plan->sub = value1;
   else
	   plan->sub = value1C;
   if(!~value2C)
	   plan->pred = value2;
   else
	   plan->pred = value2C;
   if(!~value3C)
	   plan->obj = value3;
   else
	   plan->obj = value3C;

   normalizePattern(order, plan->sub, plan->pred, plan->obj);

   plan->transfer = Plan::nomove;

   if (!~value1) {
      if (!~value2) {
         plan->ordering=value3;
      } else {
    	 scanned=getCardinality(*db,order,value1,value1C,value2,value2C,~0u,~0u,card,org_card);
    	 plan->ordering=value2;
      }
   } else {
      scanned=getCardinality(*db,order,value1,value1C,~0u,~0u,~0u,~0u,card,org_card);
      plan->ordering=value1;
   }
   plan->costs = Costs::memScan(scanned);
   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,value3);

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
void PlanGen::buildAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,BitSet &width,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C)
   // Build an aggregated index scan
{
   // Refuse placing constants at the end
   // They should be pruned out anyway, but sometimes are not due to misestimations
   //std::cout << "Aggr. Index Scan" << endl;
	if ((!~value2)&&(~value1))
      return;
   // Initialize a new plan
   Plan* plan=plans.alloc();
   plan->op=Plan::AggregatedIndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;
   plan->node_loc = BitSet();
   plan->width = BitSet(width.valueOf());

   if(!~value1C)
	   plan->sub = value1;
   else
	   plan->sub = value1C;
   if(!~value2C)
	   plan->pred = value2;
   else
	   plan->pred = value2C;

   plan->obj = ~0u;

   normalizePattern(order, plan->sub, plan->pred, plan->obj);


   // Compute the statistics
   unsigned card, org_card;
   getCardinality(*db,order,value1, value1C, value2, value2C, ~0u, ~0u, card, org_card);
   unsigned scanned=card;

   //unsigned scanned=getCardinality(*db,order,value1C,value2C,~0u);
   unsigned fullSize=db->statistics->getCardnality();
   plan->node_loc.set(getLocation(*db,order,value1C, value2C,~0u,db->getEnv()));

   if (scanned>fullSize)
      scanned=fullSize;
   if (!scanned) scanned=1;
   plan->est_cardinality=scanned;
   plan->cardinality=(card > fullSize)? fullSize: (!card)? 1:card;
   if (!~value1) {
      plan->ordering=value2;
   } else {
      plan->ordering=value1;
   }
   //unsigned pages=1+static_cast<unsigned>(db->getAggregatedFacts(order).getPages()*(static_cast<double>(scanned)/static_cast<double>(fullSize)));
   //plan->costs=Costs::seekBtree()+Costs::scan(pages);
   //plan->costs = Costs::memScan(scanned);
   plan->costs = Costs::memScan(scanned);

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,~0u);

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
void PlanGen::buildFullyAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,BitSet &width,unsigned value1,unsigned value1C)
   // Build an fully aggregated index scan
{
   // Initialize a new plan

   Plan* plan=plans.alloc();
   plan->op=Plan::FullyAggregatedIndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;
   plan->node_loc = BitSet();
   plan->width = BitSet(width.valueOf());

   if(!~value1C)
	   plan->sub = value1;
   else
	   plan->sub = value1C;

   // Compute the statistics
   unsigned card, org_card;
   getCardinality(*db,order,value1, value1C,~0u,~0u, ~0u, ~0u, card, org_card);
   unsigned scanned=card;
   //unsigned scanned=getCardinality(*db,order,value1C,~0u,~0u);
   unsigned fullSize=db->statistics->getCardnality();
   plan->node_loc.set(getLocation(*db,order,value1C, ~0u,~0u,db->getEnv()));

   if (scanned>fullSize)
      scanned=fullSize;
   if (!scanned) scanned=1;
   plan->est_cardinality=scanned;
   plan->cardinality=(card > fullSize)? fullSize: (!card)? 1:card;
   if (!~value1) {
      plan->ordering=~0u;
   } else {
      plan->ordering=value1;
   }
   //unsigned pages=1+static_cast<unsigned>(db->getFullyAggregatedFacts(order).getPages()*(static_cast<double>(scanned)/static_cast<double>(fullSize)));
   //plan->costs=Costs::seekBtree()+Costs::scan(pages);
   plan->costs = Costs::memScan(scanned);
   // Apply filters
   plan=buildFilters(plans,query,plan,value1,~0u,~0u);

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph::Filter* filter,unsigned val)
   // Check if a variable is unused
{
   if (!filter)
      return true;
   if (filter->type==QueryGraph::Filter::Variable)
      if (filter->id==val)
         return false;
   return isUnused(filter->arg1,val)&&isUnused(filter->arg2,val)&&isUnused(filter->arg3,val);
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph& query,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
	for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
	      if ((*iter)==val)
	         return false;

   return true;
}

static bool isUnused(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if (!isUnused(&(*iter),val))
         return false;
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;

      if ((&n)==(&node))
         continue;
      if ((!n.constSubject)&&(val==n.subject)) return false;
      if ((!n.constPredicate)&&(val==n.predicate)) return false;
      if ((!n.constObject)&&(val==n.object)) return false;
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
      if (!isUnused(*iter,node,val))
         return false;
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
      for (vector<QueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
         if (!isUnused(*iter2,node,val))
            return false;
   return true;
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph& query,const QueryGraph::Node& node,unsigned val, bool summary)
   // Check if a variable is unused outside its primary pattern
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      if ((*iter)==val)
         return false;

   return isUnused(query.getQuery(),node,val);
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id)
   // Generate base table accesses
{
   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   // Check which parts of the pattern are unused
   bool unusedSubject=(!node.constSubject)&&isUnused(*fullQuery,node,node.subject,db->getEnv());
   bool unusedPredicate=(!node.constPredicate)&&isUnused(*fullQuery,node,node.predicate,db->getEnv());
   bool unusedObject=(!node.constObject)&&isUnused(*fullQuery,node,node.object,db->getEnv());

   BitSet width = BitSet();
   if(!unusedSubject && !node.constSubject)
	   width.set(node.subject);
   if(!unusedPredicate && !node.constPredicate)
	   width.set(node.predicate);
   if(!unusedObject && !node.constObject)
	   width.set(node.object);

   result->variables = BitSet(width.valueOf());

   // Lookup variables
   unsigned s=node.constSubject?~0u:node.subject,p=node.constPredicate?~0u:node.predicate,o=node.constObject?~0u:node.object;
   unsigned sc=node.constSubject?node.subject:~0u,pc=node.constPredicate?node.predicate:~0u,oc=node.constObject?node.object:~0u;

   // Build all relevant scans
   if ((unusedSubject+unusedPredicate+unusedObject)>=2) {
      if (!unusedSubject) {
         buildFullyAggregatedIndexScan(query,Database::Order_Subject_Predicate_Object,result,width,s,sc);
      } else if (!unusedObject) {
         buildFullyAggregatedIndexScan(query,Database::Order_Object_Subject_Predicate,result,width,o,oc);
      } else {
         buildFullyAggregatedIndexScan(query,Database::Order_Predicate_Subject_Object,result,width,s,sc);
      }
   } else {


      if(!db->getEnv()){
	  if (unusedObject)
         buildAggregatedIndexScan(query,Database::Order_Subject_Predicate_Object,result,width,s,sc,p,pc);
      else if(!unusedPredicate)
         buildIndexScan(query,Database::Order_Subject_Predicate_Object,result,width,s,sc,p,pc,o,oc);

      if (unusedPredicate)
         buildAggregatedIndexScan(query,Database::Order_Subject_Object_Predicate,result,width,s,sc,o,oc);
      else if(!unusedObject)
         buildIndexScan(query,Database::Order_Subject_Object_Predicate,result,width,s,sc,o,oc,p,pc);
      }

      if (unusedObject)
         buildAggregatedIndexScan(query,Database::Order_Predicate_Subject_Object,result,width,p,pc,s,sc);
      else if(!unusedSubject)
       	 buildIndexScan(query,Database::Order_Predicate_Subject_Object,result,width,p,pc,s,sc,o,oc);
      if (unusedSubject)
         buildAggregatedIndexScan(query,Database::Order_Predicate_Object_Subject,result,width,p,pc,o,oc);
      else if(!unusedObject)
         buildIndexScan(query,Database::Order_Predicate_Object_Subject,result,width,p,pc,o,oc,s,sc);

      if(!db->getEnv()){
      if (unusedSubject)
         buildAggregatedIndexScan(query,Database::Order_Object_Predicate_Subject,result,width,o,oc,p,pc);
      else if(!unusedPredicate)
         buildIndexScan(query,Database::Order_Object_Predicate_Subject,result,width,o,oc,p,pc,s,sc);
      if (unusedPredicate)
         buildAggregatedIndexScan(query,Database::Order_Object_Subject_Predicate,result,width,o,oc,s,sc);
      else if(!unusedSubject)
         buildIndexScan(query,Database::Order_Object_Subject_Predicate,result,width,o,oc,s,sc,p,pc);
      }


   }
   // Update the child pointers as info for the code generation
   for (Plan* iter=result->plans;iter;iter=iter->next) {
      Plan* iter2=iter;
      while (iter2->op==Plan::Filter)
         iter2=iter2->left;
      iter2->left=static_cast<Plan*>(0)+id;
      iter2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Node*>(&node));
   }

   return result;
}
//---------------------------------------------------------------------------
PlanGen::JoinDescription PlanGen::buildJoinInfo(const QueryGraph::SubQuery& query,const QueryGraph::Edge& edge)
   // Build the informaion about a join
{
   // Fill in the relations involved
   JoinDescription result;
   result.left.set(edge.from);
   result.right.set(edge.to);

   // Compute the join selectivity
   const QueryGraph::Node& l=query.nodes[edge.from],&r=query.nodes[edge.to];
   //std::cout << l.constSubject << " " << l.subject << " " << l.constPredicate << " " << l.predicate << " " << l.constObject << " " << l.object << " " << r.constSubject << " " << r.subject << " " << r.constPredicate << " " << r.predicate << " " << r.constObject << " " << r.object << endl;
   //std::cout << l.predicate << " " << l.object << " " << r.predicate << " " << r.object << endl;
   result.selectivity=db->statistics->getJoinSelectivity(l.constSubject,l.subject,l.constPredicate,l.predicate,l.constObject,l.object,r.constSubject,r.subject,r.constPredicate,r.predicate,r.constObject,r.object);
   if(result.selectivity == 0) result.selectivity = 1;
   //std::cout << "join-selectivity:" << result.selectivity << endl;
   // Look up suitable orderings
   if (!edge.common.empty()) {
      result.ordering=edge.common.front(); // XXX multiple orderings possible
   } else {
      // Cross product
      result.ordering=(~0u)-1;
      result.selectivity=-1;
   }
   result.tableFunction=0;

   return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildOptional(const QueryGraph::SubQuery& query,unsigned id)
   // Generate an optional part
{
   // Solve the subproblem
   Plan* p=translate(query);

   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=p;
   result->relations=BitSet();
   result->relations.set(id);

   return result;
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph::Filter* filter,set<unsigned>& vars,const void* except)
   // Collect all variables used in a filter
{
   if ((!filter)||(filter==except))
      return;
   if (filter->type==QueryGraph::Filter::Variable)
      vars.insert(filter->id);
   collectVariables(filter->arg1,vars,except);
   collectVariables(filter->arg2,vars,except);
   collectVariables(filter->arg3,vars,except);
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph::SubQuery& query,set<unsigned>& vars,const void* except)
   // Collect all variables used in a subquery
{
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      collectVariables(&(*iter),vars,except);
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;
      if (except==(&n))
         continue;
      if (!n.constSubject) vars.insert(n.subject);
      if (!n.constPredicate) vars.insert(n.predicate);
      if (!n.constObject) vars.insert(n.object);
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
      if (except!=(&(*iter)))
         collectVariables(*iter,vars,except);
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
      if (except!=(&(*iter)))
         for (vector<QueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
            if (except!=(&(*iter2)))
               collectVariables(*iter2,vars,except);
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph& query,set<unsigned>& vars,const void* except)
   // Collect all variables used in a query
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      vars.insert(*iter);
   if (except!=(&(query.getQuery())))
      collectVariables(query.getQuery(),vars,except);
}
//---------------------------------------------------------------------------
static Plan* findOrdering(Plan* root,unsigned ordering)
   // Find a plan with a specific ordering
{
   for (;root;root=root->next)
      if (root->ordering==ordering)
         return root;
   return 0;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildUnion(const vector<QueryGraph::SubQuery>& query,unsigned id)
   // Generate a union part
{
   // Solve the subproblems
   vector<Plan*> parts,solutions;
   for (unsigned index=0;index<query.size();index++) {
      Plan* p=translate(query[index]),*bp=p;
      for (Plan* iter=p;iter;iter=iter->next)
         if (iter->costs<bp->costs)
            bp=iter;
      parts.push_back(bp);
      solutions.push_back(p);
   }

   // Compute statistics
   Plan::card_t card=0;
   Plan::cost_t costs=0;
   for (vector<Plan*>::const_iterator iter=parts.begin(),limit=parts.end();iter!=limit;++iter) {
      card+=(*iter)->cardinality;
      costs+=(*iter)->costs;
   }

   // Create a new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   // And create a union operator
   Plan* last=plans.alloc();
   last->op=Plan::Union;
   last->opArg=0;
   last->left=parts[0];
   last->right=parts[1];
   last->cardinality=card;
   last->costs=costs;
   last->ordering=~0u;
   last->next=0;
   result->plans=last;
   for (unsigned index=2;index<parts.size();index++) {
      Plan* nextPlan=plans.alloc();
      nextPlan->left=last->right;
      last->right=nextPlan;
      last=nextPlan;
      last->op=Plan::Union;
      last->opArg=0;
      last->right=parts[index];
      last->cardinality=card;
      last->costs=costs;
      last->ordering=~0u;
      last->next=0;
   }

   // Could we also use a merge union?
   set<unsigned> otherVars,unionVars;
   vector<unsigned> commonVars;
   collectVariables(*fullQuery,otherVars,&query);
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.begin(),limit=query.end();iter!=limit;++iter)
      collectVariables(*iter,unionVars,0);
   set_intersection(otherVars.begin(),otherVars.end(),unionVars.begin(),unionVars.end(),back_inserter(commonVars));
   if (commonVars.size()==1) {
      unsigned resultVar=commonVars[0];
      // Can we get all plans sorted in this way?
      bool canMerge=true;
      costs=0;
      for (vector<Plan*>::const_iterator iter=solutions.begin(),limit=solutions.end();iter!=limit;++iter) {
         Plan* p;
         if ((p=findOrdering(*iter,resultVar))==0) {
            canMerge=false;
            break;
         }
         costs+=p->costs;
      }
      // Yes, build the plan
      if (canMerge) {
         Plan* last=plans.alloc();
         last->op=Plan::MergeUnion;
         last->opArg=0;
         last->left=findOrdering(solutions[0],resultVar);
         last->right=findOrdering(solutions[1],resultVar);
         last->cardinality=card;
         last->costs=costs;
         last->ordering=resultVar;
         last->next=0;
         result->plans->next=last;
         for (unsigned index=2;index<solutions.size();index++) {
            Plan* nextPlan=plans.alloc();
            nextPlan->left=last->right;
            last->right=nextPlan;
            last=nextPlan;
            last->op=Plan::MergeUnion;
            last->opArg=0;
            last->right=findOrdering(solutions[index],resultVar);
            last->cardinality=card;
            last->costs=costs;
            last->ordering=resultVar;
            last->next=0;
         }
      }
   }

   return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildTableFunction(const QueryGraph::TableFunction& /*function*/,unsigned id)
   // Generate a table function access
{
   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   return result;
}
//---------------------------------------------------------------------------
static void findFilters(Plan* plan,set<const QueryGraph::Filter*>& filters)
   // Find all filters already applied in a plan
{
   switch (plan->op) {
      case Plan::Union:
      case Plan::MergeUnion:
         // A nested subquery starts here, stop
         break;
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan:
      case Plan::FullyAggregatedIndexScan:
      case Plan::Singleton:
         // We reached a leaf.
         break;
      case Plan::Filter:
         filters.insert(reinterpret_cast<QueryGraph::Filter*>(plan->right));
         findFilters(plan->left,filters);
         break;
      case Plan::NestedLoopJoin:
      case Plan::MergeJoin:
      case Plan::HashJoin:
         findFilters(plan->left,filters);
         findFilters(plan->right,filters);
         break;
      case Plan::HashGroupify: case Plan::TableFunction:
         findFilters(plan->left,filters);
         break;
   }
}


//---------------------------------------------------------------------------

Plan* PlanGen::translate(const QueryGraph::SubQuery& query)
   // Translate a query into an operator tree
{
   bool singletonNeeded=(!(query.nodes.size()+query.optional.size()+query.unions.size()))&&query.tableFunctions.size();

   // Check if we could handle the query
   if ((query.nodes.size()+query.optional.size()+query.unions.size()+query.tableFunctions.size()+singletonNeeded)>BitSet::maxWidth)
      return 0;

   // Seed the DP table with scans
   vector<Problem*> dpTable;
   dpTable.resize(query.nodes.size()+query.optional.size()+query.unions.size()+query.tableFunctions.size()+singletonNeeded);
   Problem* last=0;
   unsigned id=0;

   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter,++id) {
      Problem* p=buildScan(query,*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }

   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter,++id) {
      Problem* p=buildOptional(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter,++id) {
      Problem* p=buildUnion(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   unsigned functionIds=id;
   for (vector<QueryGraph::TableFunction>::const_iterator iter=query.tableFunctions.begin(),limit=query.tableFunctions.end();iter!=limit;++iter,++id) {
      Problem* p=buildTableFunction(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   unsigned singletonId=id;
   if (singletonNeeded) {
      Plan* plan=plans.alloc();
      plan->op=Plan::Singleton;
      plan->opArg=0;
      plan->left=0;
      plan->right=0;
      plan->next=0;
      plan->cardinality=1;
      plan->ordering=~0u;
      plan->costs=0;

      Problem* problem=problems.alloc();
      problem->next=0;
      problem->plans=plan;
      problem->relations=BitSet();
      problem->relations.set(id);
      if (last)
         last->next=problem; else
         dpTable[0]=problem;
      last=problem;
   }

   // Construct the join info
   vector<JoinDescription> joins;
   for (vector<QueryGraph::Edge>::const_iterator iter=query.edges.begin(),limit=query.edges.end();iter!=limit;++iter)
      joins.push_back(buildJoinInfo(query,*iter));
   id=functionIds;
  for (vector<QueryGraph::TableFunction>::const_iterator iter=query.tableFunctions.begin(),limit=query.tableFunctions.end();iter!=limit;++iter,++id) {
      JoinDescription join;
      set<unsigned> input;
      for (vector<QueryGraph::TableFunction::Argument>::const_iterator iter2=(*iter).input.begin(),limit2=(*iter).input.end();iter2!=limit2;++iter2)
         if (~(*iter2).id)
            input.insert((*iter2).id);
      for (unsigned index=0;index<query.nodes.size();index++) {
         unsigned s=query.nodes[index].constSubject?~0u:query.nodes[index].subject;
         unsigned p=query.nodes[index].constPredicate?~0u:query.nodes[index].predicate;
         unsigned o=query.nodes[index].constObject?~0u:query.nodes[index].object;
         if (input.count(s)||input.count(p)||input.count(o)||input.empty())
            join.left.set(index);
      }
      if (singletonNeeded&&input.empty())
         join.left.set(singletonId);
      for (unsigned index=0;index<query.tableFunctions.size();index++) {
         bool found=false;
         for (vector<unsigned>::const_iterator iter=query.tableFunctions[index].output.begin(),limit=query.tableFunctions[index].output.end();iter!=limit;++iter)
            if (input.count(*iter)) {
               found=true;
               break;
            }
         if (found)
            join.left.set(functionIds+index);
      }
      join.right.set(id);
      join.ordering=~0u;
      join.selectivity=1;
      join.tableFunction=&(*iter);
      joins.push_back(join);
   }

   // Build larger join trees
   vector<unsigned> joinOrderings;
   unsigned alternate_join = false;
   for (unsigned index=1;index<dpTable.size();) {
      map<BitSet,Problem*> lookup;
      unsigned index2 = (db->getEnv())? (index - 1):0;
      for (;index2<index;index2++) {
    	  //if(db->getEnv() && index2 > 0) break;
    	  //std::cout << ":";
    	  for (Problem* iter=dpTable[index2];iter;iter=iter->next) {
            BitSet leftRel=iter->relations;
            for (Problem* iter2=dpTable[index-index2-1];iter2;iter2=iter2->next) {
               // Overlapping subproblem?
               BitSet rightRel=iter2->relations;
               if (leftRel.overlapsWith(rightRel))
                  continue;

               // Investigate all join candidates
               Problem* problem=0;
               double selectivity=1;
               for (vector<JoinDescription>::const_iterator iter3=joins.begin(),limit3=joins.end();iter3!=limit3;++iter3){
                  //if (((*iter3).left.subsetOf(leftRel))&&((*iter3).right.subsetOf(rightRel))) {
		 bool test;
                 if(!alternate_join) test =(((*iter3).left.subsetOf(leftRel)) && ((*iter3).right.subsetOf(rightRel)));
                 else test= test || (((*iter3).left.subsetOf(rightRel)) && ((*iter3).right.subsetOf(leftRel)));
                 if(test){

                     if (!iter->plans) break;
                     // We can join it...
                     BitSet relations=leftRel.unionWith(rightRel);
                     if (lookup.count(relations)) {
                        problem=lookup[relations];
                     } else {
                        lookup[relations]=problem=problems.alloc();
                        problem->relations=relations;
                        problem->plans=0;
                        problem->next=dpTable[index];
                        dpTable[index]=problem;
                     }
                     // Table function call?
                     if ((*iter3).tableFunction) {
                        for (Plan* leftPlan=iter->plans;leftPlan;leftPlan=leftPlan->next) {
                           Plan* p=plans.alloc();
                           p->op=Plan::TableFunction;
                           p->opArg=0;
                           p->left=leftPlan;
                           p->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::TableFunction*>((*iter3).tableFunction));
			   p->next=0;
                           p->cardinality=leftPlan->cardinality;
                           p->costs=leftPlan->costs+Costs::tableFunction(leftPlan->cardinality);
                           p->ordering=leftPlan->ordering;
                           addPlan(problem,p);
                        }

                        problem=0;
                        break;
                     }
                     // Collect selectivities and join order candidates
                     joinOrderings.clear();
                     joinOrderings.push_back((*iter3).ordering);
                     selectivity=(*iter3).selectivity;
                     for (++iter3;iter3!=limit3;++iter3) {
                        joinOrderings.push_back((*iter3).ordering);
                        // selectivity*=(*iter3).selectivity;
                     }
                     break;
                  }
               }
               if (!problem) continue;

               // Combine phyiscal plans
               for (Plan* leftPlan=iter->plans;leftPlan;leftPlan=leftPlan->next) {
                  for (Plan* rightPlan=iter2->plans;rightPlan;rightPlan=rightPlan->next) {

                	  //TODO: Replace with optimal loop
                	  /// Finds the left & right relation widths
                	  unsigned leftWidthSize = 0, rightWidthSize = 0;

                	  BitSet newWidth = BitSet(leftPlan->width.valueOf());
                	  newWidth = newWidth.unionWith(rightPlan->width);
                	  BitSet restWidth = BitSet();

                	  if(!db->getEnv()) //If not summary prune variables
                	  for(Problem *it = dpTable[0]; it; it = it->next){
                	  		if(!it->relations.subsetOf(problem->relations))
                	  			restWidth = restWidth.unionWith(it->variables);

                	  }

                	  newWidth = newWidth.intersectWith(restWidth);
                	  for(unsigned id = 0; id < BitSet::maxWidth; id++){
                		  if((leftPlan->width.test(id) || rightPlan->width.test(id)) && (db->getEnv() || !isUnused(*fullQuery,id)))
                			  newWidth.set(id);
                	  }
                	  for(unsigned id = 0; id < BitSet::maxWidth; id++){
                		  if(leftPlan->width.test(id))
                			  leftWidthSize++;
                		  if(rightPlan->width.test(id))
                		      rightWidthSize++;

                	  }

                	  Plan::Transfer ship; double shipCosts; BitSet join_loc;

                      double leftRelSize = leftPlan->cardinality * leftWidthSize;
                      double rightRelSize = rightPlan->cardinality * rightWidthSize;
                	  unsigned left_loc = leftPlan->node_loc.valueOf();
                	  unsigned right_loc = rightPlan->node_loc.valueOf();
                      if(!~left_loc && !~right_loc){
                		  ship = Plan::nomove;
                		  shipCosts = 0;
                		  join_loc = BitSet();
                		  join_loc.set(~0u);
                	  }else{
                		  if(leftPlan->node_loc == rightPlan->node_loc){
                			  ship = Plan::nomove;
                			  shipCosts = 0;
                			  join_loc = BitSet(leftPlan->node_loc.valueOf());
                		  }

				else if(leftRelSize <= rightRelSize){
                			  ship = Plan::left2right;
                			  shipCosts = Costs::transferCosts(leftRelSize);
                			  if(~right_loc){
                				  join_loc = BitSet(rightPlan->node_loc.valueOf());
                				  shipCosts = Costs::transferCosts(leftRelSize/db->clusterSize);
                			  }
                			  else{
                				  join_loc = BitSet(~0u);
                				  //shipCosts = Costs::transferCosts(leftRelSize);
                			  }
                		  }
                		  else{
                			  ship = Plan::right2left;
                			  shipCosts = Costs::transferCosts(rightRelSize);
                			  if(~left_loc){
                				join_loc = BitSet(leftPlan->node_loc.valueOf());
                    			  	shipCosts = Costs::transferCosts(rightRelSize/db->clusterSize);
                			  }
                			  else{
                				join_loc = BitSet(~0u);
                    			  	//shipCosts = Costs::transferCosts(rightRelSize);
                			  }
                		  }
                	  }


/*
                      double leftRelSize = leftPlan->cardinality * leftWidthSize;
                      double rightRelSize = rightPlan->cardinality * rightWidthSize;
                	  if(leftPlan->node_loc == rightPlan->node_loc){
                    	  ship = Plan::nomove;
                    	  costs = leftPlan->costs+rightPlan->costs;
                    	  join_loc = BitSet(leftPlan->node_loc.valueOf());
                	  }else{
                		  double costs1 = std::max(leftPlan->costs, rightPlan->costs+Costs::transferCosts(rightRelSize));
                		  double costs2 = std::max(rightPlan->costs, leftPlan->costs+Costs::transferCosts(leftRelSize));
                		  if(costs1 < costs2){
                        	  ship = Plan::right2left;
                        	  costs = costs1;
                        	  join_loc = BitSet(leftPlan->node_loc.valueOf());
                		  }else{
                        	  ship = Plan::left2right;
                        	  costs = costs2;
                        	  join_loc = BitSet(rightPlan->node_loc.valueOf());
                		  }
                	  }
*/
                  
                      // Try a merge joins
                      //std::cout << leftPlan->ordering << "###" << rightPlan->ordering << endl;
                      if (db->getEnv() || (leftPlan->ordering==rightPlan->ordering)) {
                         for (vector<unsigned>::const_iterator iter=joinOrderings.begin(),limit=joinOrderings.end();iter!=limit;++iter) {
                            if (leftPlan->ordering==(*iter)) {
                               Plan* p=plans.alloc();
                               p->op=Plan::MergeJoin;
                               p->opArg=*iter;
                               p->left=leftPlan;
                               p->right=rightPlan;
                               p->next=0;
                               //std::cout << leftPlan->cardinality << "--" << rightPlan->cardinality << endl;
                               if ((p->cardinality=(leftPlan->cardinality*rightPlan->cardinality)*selectivity)<1) p->cardinality=1;
 			       p->costs=(leftPlan->costs+rightPlan->costs)+Costs::mergeJoin(leftPlan->cardinality,rightPlan->cardinality);
                               //p->costs=std::max(leftPlan->costs,rightPlan->costs)+Costs::mergeJoin(leftPlan->cardinality,rightPlan->cardinality);
                               p->costs=p->costs + shipCosts;
                               p->transfer = ship;
                               p->node_loc = join_loc;
                               p->ordering=leftPlan->ordering;
                               p->width = BitSet(newWidth.valueOf());
                               addPlan(problem,p);
                               break;
                            }
                         }
                      }
                      // Try a hash join
                      if (selectivity>=0) {
                         Plan* p=plans.alloc();
                         p->op=Plan::HashJoin;
                         p->opArg=0;
                         p->left=leftPlan;
                         p->right=rightPlan;
                         p->next=0;
                         p->width = BitSet(newWidth.valueOf());
                         p->transfer = ship;
                         p->node_loc = join_loc;
                         
                                            
 						//if (summary && ((p->cardinality=(leftPlan->cardinality+rightPlan->cardinality)*selectivity)<1)) p->cardinality=1;
                         if ((p->cardinality=(leftPlan->cardinality*rightPlan->cardinality)*selectivity)<1) p->cardinality=1;
                         p->costs=std::max(leftPlan->costs,rightPlan->costs)+Costs::hashJoin(leftPlan->cardinality,rightPlan->cardinality);
                         p->costs=p->costs + shipCosts;
                         p->ordering=~0u;
                         addPlan(problem,p);
                         // Second order
                         p=plans.alloc();
 						p->transfer = ship;
 						p->width = BitSet(newWidth.valueOf());
                         p->node_loc = join_loc;
                         p->op=Plan::HashJoin;
                         p->opArg=0;
                         p->left=rightPlan;
                         p->right=leftPlan;
 			p->next=0;
 						//if (summary && ((p->cardinality=(leftPlan->cardinality+rightPlan->cardinality)*selectivity)<1)) p->cardinality=1;
 						if ((p->cardinality=(leftPlan->cardinality*rightPlan->cardinality)*selectivity)<1) p->cardinality=1;
                         p->costs=std::max(leftPlan->costs,rightPlan->costs)+Costs::hashJoin(rightPlan->cardinality,leftPlan->cardinality);
                         p->costs=p->costs + shipCosts;
                         p->ordering=~0u;
                         addPlan(problem,p);
                      } else {
                         // Nested loop join
                         Plan* p=plans.alloc();
                         p->op=Plan::NestedLoopJoin;
                         p->opArg=0;
                         p->left=leftPlan;
                         p->right=rightPlan;
 			p->next=0;
 						//if (summary && ((p->cardinality=(leftPlan->cardinality+rightPlan->cardinality)*selectivity)<1)) p->cardinality=1;
 						 if ((p->cardinality=(leftPlan->cardinality*rightPlan->cardinality)*selectivity)<1) p->cardinality=1;
                         p->costs=std::max(leftPlan->costs,rightPlan->costs)+leftPlan->cardinality*rightPlan->costs;
                         p->costs=p->costs + shipCosts;
                         p->transfer = ship;
                         p->node_loc = join_loc;
                         p->ordering=leftPlan->ordering;
 						p->width = BitSet(newWidth.valueOf());
                         addPlan(problem,p);
                     }
                  }
               }
            }
         }
      }
      if(!dpTable[index]) alternate_join = !alternate_join;
      else  alternate_join = false;
      if(!alternate_join)index++;

   }
   // Extract the bestplan
   if (dpTable.empty()||(!dpTable.back()))
      return 0;
   Plan* plan=dpTable.back()->plans;
   if (!plan)
      return 0;

   // Add all remaining filters
   set<const QueryGraph::Filter*> appliedFilters;
   findFilters(plan,appliedFilters);
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if (!appliedFilters.count(&(*iter))) {
         Plan* p=plans.alloc();
         p->op=Plan::Filter;
         p->opArg=0;
         p->left=plan;
         p->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
	 p->next=0;
         p->cardinality=plan->cardinality; // XXX real computation
         p->costs=plan->costs;
         p->ordering=plan->ordering;
         plan=p;
      }

   // Return the complete plan
   return plan;
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate(Database* db, const QueryGraph& query)
   // Translate a query into an operator tree
{
   // Reset the plan generator
   plans.clear();
   problems.freeAll();
   fullQuery=&query;
   this->db = db;
   // Retrieve the base plan
   Plan* plan=translate(query.getQuery());

   if (!plan)
      return 0;
   Plan* best=0;
   for (Plan* iter=plan;iter;iter=iter->next)
      if ((!best)||(iter->costs<best->costs)||((iter->costs==best->costs)&&(iter->cardinality<best->cardinality)))
         best=iter;
   if (!best)
      return 0;

   // Aggregate, if required
   if ((query.getDuplicateHandling()==QueryGraph::CountDuplicates)||(query.getDuplicateHandling()==QueryGraph::NoDuplicates)||(query.getDuplicateHandling()==QueryGraph::ShowDuplicates)) {
      Plan* p=plans.alloc();
      p->op=Plan::HashGroupify;
      p->opArg=0;
      p->left=best;
      p->right=0;
      p->next=0;
      p->cardinality=best->cardinality; // not correct, be we do not use this value anyway
      p->costs=best->costs; // the same here
      p->ordering=~0u;
      best=p;
   }

   return best;
}
//---------------------------------------------------------------------------

//---------------------------------------------------------------------------
// batch the required cardinalities and selectivities for plan generation
void PlanGen::prerequistes(Database* db, const QueryGraph & query, vector<unsigned> & vect){

    unsigned id=0;
    fullQuery=&query;
    this->db = db;
    QueryGraph::SubQuery sub_query =  query.getQuery();

	// scan costs request
	 for (vector<QueryGraph::Node>::const_iterator iter=sub_query.nodes.begin(),limit=sub_query.nodes.end();iter!=limit;++iter,++id){
		 preBuildScan(sub_query,*iter,id,vect);
	 }
}
static void pre_getCardinality(Database& db, Database::DataOrder order,
		unsigned v1, unsigned c1, unsigned v2, unsigned c2, unsigned v3,
		unsigned c3, unsigned &card, unsigned &org_card, vector<unsigned> & vect)
   // Estimate the cardinality of a predicate
{
   //std::cout << order << "::" << v1 << " " << c1 << "--" << v2 << " " << c2 << "--"<< v3 << " " << c3 << " " << card << endl;
   db.statistics->getCardinality(v1,c1,v2,c2,v3,c3, card, org_card, order, vect);
}



void PlanGen::preBuildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id, vector<unsigned> & vect){

   // Check which parts of the pattern are unused
   bool unusedSubject=(!node.constSubject)&&isUnused(*fullQuery,node,node.subject,db->getEnv());
   bool unusedPredicate=(!node.constPredicate)&&isUnused(*fullQuery,node,node.predicate,db->getEnv());
   bool unusedObject=(!node.constObject)&&isUnused(*fullQuery,node,node.object,db->getEnv());

   // Lookup variables
   unsigned s=node.constSubject?~0u:node.subject,p=node.constPredicate?~0u:node.predicate,o=node.constObject?~0u:node.object;
   unsigned sc=node.constSubject?node.subject:~0u,pc=node.constPredicate?node.predicate:~0u,oc=node.constObject?node.object:~0u;

   // Build all relevant scans
   unsigned card, org_card;
   if ((unusedSubject+unusedPredicate+unusedObject)>=2) {
       if (!unusedSubject) {
    	   pre_getCardinality(*db,Database::Order_Subject_Predicate_Object,s, sc,~0u,~0u, ~0u, ~0u, card, org_card,vect);
       } else if (!unusedObject) {
    	   pre_getCardinality(*db,Database::Order_Object_Subject_Predicate,s, sc,~0u,~0u, ~0u, ~0u, card, org_card,vect);
       } else {
    	   pre_getCardinality(*db,Database::Order_Object_Subject_Predicate,s, sc,~0u,~0u, ~0u, ~0u, card, org_card,vect);
       }
   }else {

	   if (unusedObject)
		   pre_getCardinality(*db,Database::Order_Subject_Predicate_Object,s, sc, p, pc, ~0u, ~0u, card, org_card,vect);
	   else{
		   pre_getCardinality(*db,Database::Order_Subject_Predicate_Object,s, sc, p, pc, o, oc, card, org_card,vect);
		   pre_getCardinality(*db,Database::Order_Subject_Predicate_Object,s, sc, p, pc, ~0u, ~0u, card, org_card,vect);
		   pre_getCardinality(*db,Database::Order_Subject_Predicate_Object,s, sc, ~0u, ~0u, ~0u, ~0u, card, org_card,vect);
	   }

      if (unusedPredicate)
    	  pre_getCardinality(*db,Database::Order_Subject_Object_Predicate,s, sc, o, oc, ~0u, ~0u, card, org_card,vect);
      else{
    	  pre_getCardinality(*db,Database::Order_Subject_Object_Predicate,s, sc, o, oc, p, pc, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Subject_Object_Predicate,s, sc, o, oc, ~0u, ~0u, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Subject_Object_Predicate,s, sc, ~0u, ~0u, ~0u, ~0u, card, org_card,vect);
      }
      if (unusedObject)
    	  pre_getCardinality(*db,Database::Order_Predicate_Subject_Object,p, pc, s, sc, ~0u, ~0u, card, org_card,vect);
      else{
    	  pre_getCardinality(*db,Database::Order_Predicate_Subject_Object,p, pc, s, sc, o, oc, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Predicate_Subject_Object,p, pc, s, sc, ~0u, ~0u, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Predicate_Subject_Object,p, pc, ~0u, ~0u, ~0u, ~0u, card, org_card,vect);
      }

      if (unusedSubject)
    	  pre_getCardinality(*db,Database::Order_Predicate_Object_Subject,p, pc, o, oc, ~0u, ~0u, card, org_card,vect);
      else{
    	  pre_getCardinality(*db,Database::Order_Predicate_Object_Subject,p, pc, o, oc, s, sc, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Predicate_Object_Subject,p, pc, o, oc, ~0u, ~0u, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Predicate_Object_Subject,p, pc, ~0u, ~0u, ~0u, ~0u, card, org_card,vect);
      }

      if (unusedSubject)
    	  pre_getCardinality(*db,Database::Order_Object_Predicate_Subject,o, oc, p, pc, ~0u, ~0u, card, org_card,vect);
      else{
    	  pre_getCardinality(*db,Database::Order_Object_Predicate_Subject,o, oc, p, pc, s, sc, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Object_Predicate_Subject,o, oc, p, pc, ~0u, ~0u, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Object_Predicate_Subject,o, oc, ~0u, ~0u, ~0u, ~0u, card, org_card,vect);
      }

      if (unusedPredicate)
    	  pre_getCardinality(*db,Database::Order_Object_Subject_Predicate,o, oc, s, sc, ~0u, ~0u, card, org_card,vect);
      else{
    	  pre_getCardinality(*db,Database::Order_Object_Subject_Predicate,o, oc, s, sc, p, pc, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Object_Subject_Predicate,o, oc, s, sc, ~0u, ~0u, card, org_card,vect);
    	  pre_getCardinality(*db,Database::Order_Object_Subject_Predicate,o, oc, ~0u, ~0u, ~0u, ~0u, card, org_card,vect);
      }
   }
}


