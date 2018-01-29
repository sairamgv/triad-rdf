#ifndef H_cts_semana_SemanticAnalysis
#define H_cts_semana_SemanticAnalysis
//---------------------------------------------------------------------------
#include <string>
#include <set>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include "cts/semana/SemanticAnalysis.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/infra/QueryGraph.hpp"
#include <parser/Type.hpp>
#include <database/Database.hpp>

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
class Database;
class DictionarySegment;
class DifferentialIndex;
class SPARQLParser;
class QueryGraph;
//---------------------------------------------------------------------------
/// Semantic anaylsis for SPARQL queries. Transforms the parse result into a query graph
class SemanticAnalysis
{
   public:
   /// A semantic exception
   struct SemanticException {
      /// The message
      std::string message;

      /// Constructor
      SemanticException(const std::string& message);
      /// Constructor
      SemanticException(const char* message);
      /// Destructor
      ~SemanticException();
   };
   Dictionary* dict;

   bool summary;

   private:
   /// The dictionary. Used for string and IRI resolution
   /// The differential index (if any)
   //DifferentialIndex* diffIndex;

/*
   bool lookup(const std::string& text,::Type::ID type,unsigned subType,unsigned& id);

   bool encode(const SPARQLParser::Element& element,unsigned& id,bool& constant);

   bool binds(const SPARQLParser::PatternGroup& group,unsigned id);

   bool encodeFilter(const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::Filter& output);

   bool encodeUnaryFilter(QueryGraph::Filter::Type type,const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::Filter& output);

   bool encodeBinaryFilter(QueryGraph::Filter::Type type,const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::Filter& output);

   bool encodeTernaryFilter(QueryGraph::Filter::Type type, const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::Filter& output);

   bool encodeFilter(const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::Filter& output);

   bool encodeFilter(const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::SubQuery& output);

   void encodeTableFunction(const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::SubQuery& output);

   bool transformSubquery(const SPARQLParser::PatternGroup& group,QueryGraph::SubQuery& output);
*/

   public:
   /// Constructor
//   explicit SemanticAnalysis(Database& db);
   /// Constructor
//   explicit SemanticAnalysis(DifferentialIndex& diffIndex);
   /// Constructor
   explicit SemanticAnalysis(Dictionary* dictionary);

   explicit SemanticAnalysis(Dictionary* dictionary, bool sum);

   /// Perform the transformation
   void transform(const SPARQLParser& input,QueryGraph& output);


};
//---------------------------------------------------------------------------
#endif
