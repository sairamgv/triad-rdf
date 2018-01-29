#ifndef H_cts_plangen_Costs
#define H_cts_plangen_Costs
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
/// A cost model used by the plan generator
class Costs
{
   public:
   /// Data type for costs
   typedef double cost_t;

   private:
   /// Costs for a seek in 1/10ms
   static const unsigned seekCosts = 95;
   /// Costs for a sequential page read in 1/10ms
   static const unsigned scanCosts = 17;
   /// Number of CPU operations per 1/10ms
   static const unsigned cpuSpeed = 100000;
   /// Network Speed (Time taken to transfer for integral)
   static const unsigned networkCosts = 1000000;

   public:
   /// Costs for traversing a btree
   static cost_t seekBtree() { return 3*seekCosts; }
   /// Costs for scanning a number of pages
   static cost_t scan(unsigned pages) { return pages*scanCosts; }

   /// Costs for scanning a relation in main memory
   static cost_t memScan(double triples) { return (triples/cpuSpeed); }
   /// Costs for shipping a relation to *another* node
   static cost_t transferCosts(double relationSize) { return (relationSize/networkCosts);}

   /// Costs for a merge join
   static cost_t mergeJoin(double leftCard,double rightCard) { return (leftCard/cpuSpeed)+(rightCard/cpuSpeed); }
   /// Costs for a hash join
   static cost_t hashJoin(double leftCard,double rightCard) { return 30000 + (leftCard/1000)+(rightCard/cpuSpeed); }
   /// Costs for a filter
   static cost_t filter(double card) { return card/(cpuSpeed/3); }
   /// Costs for a table function
   static cost_t tableFunction(double leftCard) { return leftCard*10000.0; }
};
//---------------------------------------------------------------------------
#endif
