/*
Copyright 2014 Sairam Gurajada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "QueryEngine.hpp"

QueryEngine::QueryEngine() {
	// TODO Auto-generated constructor stub
}

QueryEngine::~QueryEngine() {
	// TODO Auto-generated destructor stub
}

LoggerPtr QueryEngine::logger(Logger::getLogger("TriAD"));


bool QueryEngine::smallAddressSpace()

{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
string QueryEngine::readInput(istream& in)
   // Read a stream into a string
{
   string result;
   while (true) {
      string s;
      getline(in,s);
      result+=s;
      if (!in.good())
         break;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
bool QueryEngine::readLine(string& query)
   // Read a single line
{
#ifdef CONFIG_LINEEDITOR
   // Use the lineeditor interface
   static lineeditor::LineInput editHistory(L">");
   return editHistory.readUtf8(query);
#else
   // Default fallback
   cerr << ">"; cerr.flush();
   return getline(cin,query);
#endif
}
//---------------------------------------------------------------------------
void QueryEngine::showHelp()
   // Show internal commands
{
   cout << "Recognized commands:" << endl
        << "help          shows this help" << endl
        << "select ...    runs a SPARQL query" << endl
        << "explain ...   shows the execution plan for a SPARQL query" << endl
        << "exit          exits the query interface" << endl;
}
//---------------------------------------------------------------------------



//  <Distributed Query Execution>


void  QueryEngine::shipRelation(Relation* rel, vector<Channel> &channels, unsigned rank, unsigned jpos, unsigned loc, unsigned nodeid){

	TaskManager& tm = TaskManager::getInstance();
	boost::mpi::communicator& world = tm.world();
	unsigned iStatus;
	getCopy(rank,"status",iStatus);
        //std::cout << "shipping relation..." << endl;
	rel->partitionTuples(jpos,channels.size()-1,rank, loc);

	std::vector<boost::mpi::request> reqs;
		reqs.clear();

	bool turn;
	//Send Relation to peers
	for (int i = 1; i < world.size(); i++) {
		if(i == rank || !(iStatus&(1<<i))) continue;
		unsigned tag = rank << 16 | nodeid;
		reqs.push_back(world.isend(i, tag, rel->c_tuples[i]));
	}

	//Receive Relation from peers
	vector<unsigned> tuples[world.size()];
	for (int i = 1; i < world.size(); i++) {
		if(i == rank || !(iStatus&(1<<i))) continue;
		unsigned tag = i << 16 | nodeid;
		reqs.push_back(world.irecv(i, tag, tuples[i]));
	}

	boost::mpi::wait_all(reqs.begin(), reqs.end());

	for(int i = 1; i < world.size(); i++){
		if(i == rank) continue;
		if(tuples[i].size() > 0)
			rel->append(tuples[i]);
	}
	//std::cout << "done..." << endl;
}

static void getLocation(BitSet* b, unsigned & loc){
	for(loc = 0; loc < b->maxWidth; loc++)
		if(b->test(loc))
			break;
}

void QueryEngine::createThread(boost::promise<Relation*> & ret, Database *db, Plan* plan, vector<Channel> &channels, unsigned rank){
	Relation *rel = executeQuery(db,plan, channels, rank,false);
	ret.set_value(rel);
}


Relation* QueryEngine::executeQuery(Database *db, Plan* plan, vector<Channel> &channels, unsigned rank, bool full){
	if(!plan)
		return 0;
	Relation *left = 0, *right = 0;
	//std::cout << rank <<  " A" << endl;
	if(plan->op == Plan::MergeJoin || plan->op == Plan::HashJoin){
		//left = executeQuery(db,plan->left, channels, rank,false); // Single threaded
		{ // Multi-threaded
			boost::promise<Relation*> ret;
			boost::thread workerThread(&QueryEngine::createThread,this,boost::ref(ret),db,plan->left,channels,rank);
			right = executeQuery(db,plan->right,channels,rank,false);
			workerThread.join();
			left = 	ret.get_future().get();
		}
		if(!left || ! right) return 0;
	}

	if(plan->op == Plan::IndexScan || plan->op == Plan::AggregatedIndexScan){
		Relation* scan = Operator::indexScan(db,static_cast<Database::DataOrder>(plan->opArg),&plan->width,plan->sub,plan->pred,plan->obj,full,0);
		return scan;
	}else{
		unsigned left_jpos = -1, right_jpos = -1;
			for(int i = 0; i< BitSet::maxWidth; i++){
				if(plan->left->width.test(i)) left_jpos++;
				if(plan->right->width.test(i)) right_jpos++;
				if(plan->left->width.test(i) && plan->right->width.test(i))
					break;
		}
		unsigned exec_loc = ~0u;
		unsigned temp = plan->node_loc.valueOf();
		if(~temp)
			getLocation(&plan->node_loc,exec_loc);

	        //std::cout << rank <<  " B" << endl;
		if(plan->op == Plan::MergeJoin){
			if(left->getWidth() == 1 && right->getWidth() == 1){
				if(plan->transfer == Plan::left2right)
					shipRelation(left,channels,rank,left_jpos,exec_loc,plan->id);
				else if(plan->transfer == Plan::right2left)
					shipRelation(right,channels,rank,right_jpos,exec_loc,plan->id);
			}else{
				if(left->getWidth() == 1)
					shipRelation(left,channels,rank,left_jpos,exec_loc,plan->id);  // Ship Left Relation
				if(right->getWidth() == 1)
					shipRelation(right,channels,rank,right_jpos,exec_loc,plan->id);  // Ship Right Relation
			}
			if(left && right){
				rg::Timer mjtimer;
				mjtimer.start();
				Relation *mjoin = Operator::mergeJoin(left,right, &plan->left->width, &plan->right->width, &plan->width);
				mjtimer.stop();
				timer.stop();
				if(!mjoin){LOG4CXX_INFO(logger,"Node-" << rank << ": Merge Join (size:<empty>) in time: " << mjtimer);}
				else{
					LOG4CXX_INFO(logger, "Node-" << rank << ": Merge Join (size: "<< mjoin->numTuples << ") in time: " << mjtimer);
				}
				timer.resume();
				return mjoin;
			}

		}else if(plan->op == Plan::HashJoin){
			// TODO: Optimize for case at-least "ONE" ship required
			rg::Timer t1,t2;
			shipRelation(left,channels,rank,left_jpos,exec_loc,plan->id);  // Ship Left Relation
			shipRelation(right,channels,rank,right_jpos,exec_loc,plan->id);  // Ship Right Relation
			if (left && right) {
				rg::Timer hjtimer;
				hjtimer.start();
				Relation *hjoin = Operator::hashJoin(left, right, &plan->left->width, &plan->right->width, &plan->width);
				hjtimer.stop();
				timer.stop();
				if (!hjoin) {LOG4CXX_INFO(logger,"Node-" << rank << ": Hash Join (size:<empty>) in time: " << hjtimer);}
				else {LOG4CXX_INFO(logger, "Node-" << rank << ": Hash Join (size: "<< hjoin->numTuples << ") in time: " << hjtimer);
				}
				timer.resume();
				return hjoin;
			}
		}
	}
	return 0;
}
//  </Distributed Query Execution>

//=====================================================================

// <Summary Query Execution>
Relation* QueryEngine::executeQuery(Database *db, Plan* plan){

	vector<Relation*> splan;
	plan->print(0);
	generateSPhysicalPlan(db,plan,splan,0);
	return Operator::getBindings(splan,plan->width.len(),db->numPartitions);

}



void QueryEngine::generateSPhysicalPlan(Database *db, Plan* plan, vector<Relation*> & splan, unsigned joinpos){
	if(!plan)
		return;

	if(plan->op == Plan::HashJoin || plan->op == Plan::MergeJoin){
		if(plan->left && plan->right){
			if((plan->left->op == Plan::IndexScan || plan->left->op == Plan::AggregatedIndexScan)){
				if((plan->right->op == Plan::IndexScan || plan->right->op == Plan::AggregatedIndexScan)){
					if(plan->left->cardinality > plan->right->cardinality){
						Plan* temp = plan->left;
						plan->left = plan->right;
						plan->right = temp;
					}
				}else{
					Plan* temp = plan->left;
					plan->left = plan->right;
					plan->right = temp;
				}
			}

		}

		for(joinpos = 0; joinpos< BitSet::maxWidth; joinpos++)
			if(plan->left->width.test(joinpos) && plan->right->width.test(joinpos))
				break;
		generateSPhysicalPlan(db,plan->left,splan,joinpos);
		generateSPhysicalPlan(db,plan->right,splan,joinpos);
	}

	if((plan->op == Plan::IndexScan || plan->op == Plan::AggregatedIndexScan)){

		if(plan->sub == joinpos && plan->obj < 100)
			plan->opArg = Database::Order_Predicate_Subject_Object;
		else if(plan->obj == joinpos && plan->sub < 100)
			plan->opArg = Database::Order_Predicate_Object_Subject;

		Relation *scan = Operator::indexScan(db,static_cast<Database::DataOrder>(plan->opArg),&plan->width,plan->sub,plan->pred,plan->obj,false, joinpos);
		splan.push_back(scan);
	}
	return;

}

//  </Summary Query Execution>

//====================================================================

// <Intialization and plan generation>


void QueryEngine::init(Database *db) {
	// Add some initializations (if required)

	this->db = db;
	dict = db->dictionary;

}

void QueryEngine::printResults(vector<unsigned> &results, unsigned width){

	unsigned count = 0;
	std::cout << "Results: " << endl << "| " ;
	for(vector<Utilities::value32_t>::iterator it = results.begin(); it != results.end(); it++){
			std::cout << dict->lookupText(*it) << " | ";
			if(++count%width == 0)
				std::cout << endl << "| ";
		}
}


bool QueryEngine::summaryStep(string &query, VECTOR_INT_32 &plist){

	bool explain = false;

	if (query.substr(0, 8) == "explain " && query.substr(0, 7) == "EXPLAIN ") {
		query = query.substr(8);
		explain = true;

	} else if (query.substr(0, 7) != "select " 	&& query.substr(0, 7) != "SELECT ") {
		std: cout << "Query:" << query.substr(0, 7) << "| Expecting SPARQL query" << endl;
		return 0;
	}

	QueryGraph squeryGraph;
	{
		timer.reset();
		// Parse the query
		SPARQLLexer lexer(query);
		SPARQLParser parser(lexer);
		try {
			parser.parse();
		} catch (const SPARQLParser::ParserException& e) {
			LOG4CXX_ERROR(logger, "parse error: " << e.message);
			return 0;
		}
		timer.stop();
		LOG4CXX_INFO(logger, "Query parsed in " << timer);

		// And perform the semantic anaylsis
		timer.start();
		try {
			SemanticAnalysis semana(dict,true);
			semana.transform(parser,squeryGraph);
		} catch (const SemanticAnalysis::SemanticException& e) {
			LOG4CXX_ERROR(logger,"semantic error: " << e.message);
			return 0;
		}
		timer.stop();
		LOG4CXX_INFO(logger, "Semantic analysis in " << timer);
		if (squeryGraph.knownEmpty()) {
			if (explain)
				cerr << "static analysis determined that the query result will be empty" << endl; else
					cout << "<empty result>" << endl;
			return 0;
		}

		//Summary Query execution
		timer.start();
		// Set summary mode!
		db->setEnv(true);
		PlanGen splangen;
		Plan* splan=splangen.translate(db,squeryGraph);
		timer.stop();
		LOG4CXX_INFO(logger, "Plan generated in " << timer);
		LOG4CXX_INFO(logger, "<Summary plan>............................");
		splan->print(0);
		LOG4CXX_INFO(logger, "</Summary plan>............................");
		timer.start();
		Relation *srel = executeQuery(db,splan);
		srel->partitionList(plist,db->statistics->plist);
		timer.stop();
		LOG4CXX_INFO(logger, "Summary query processing in: " << timer);
	}
}

bool QueryEngine::buildStatRequest(string &query, VECTOR_INT_32 &reqVect){

	bool explain = false;


	QueryGraph queryGraph;
  {
     rg::Timer timer;
     timer.start();
	  // Parse the query
     SPARQLLexer lexer(query);
     SPARQLParser parser(lexer);
     try {
        parser.parse();
     } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        return 0;
     }
     timer.stop();
     LOG4CXX_INFO(logger, "parsed in " << timer);

     // And perform the semantic anaylsis
     timer.start();
     try {
        SemanticAnalysis semana(dict,false);
        semana.transform(parser,queryGraph);
     } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        return 0;
     }
     timer.stop();
     LOG4CXX_INFO(logger, "Semantic analysis in " << timer);
     if (queryGraph.knownEmpty()) {
        if (explain)
           cerr << "static analysis determined that the query result will be empty" << endl; else
           cout << "<empty result>" << endl;
        return 0;
     }

      timer.start();
      // Data mode
      db->setEnv(false);
      PlanGen plangen;
      plangen.prerequistes(db,queryGraph,reqVect);
/*
      for(vector<unsigned>::iterator it = reqVect.begin(); it != reqVect.end(); it++){
    	  std::cout << *it << " ";
      }
*/
      std::cout << endl;
      timer.stop();
      return true;
  }

}

bool QueryEngine::PlanGenerator(string &query, VECTOR_INT_32& plan_serialize){

	bool explain = false;

	QueryGraph queryGraph;
  {
     rg::Timer timer;
     timer.start();
	  // Parse the query
     SPARQLLexer lexer(query);
     SPARQLParser parser(lexer);
     try {
        parser.parse();
     } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        return 0;
     }
     timer.stop();
     LOG4CXX_INFO(logger, "parsed in " << timer);

     // And perform the semantic anaylsis
     timer.start();
     try {
        SemanticAnalysis semana(dict,false);
        semana.transform(parser,queryGraph);
     } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        return 0;
     }
     timer.stop();
     LOG4CXX_INFO(logger, "Semantic analysis in " << timer);
     if (queryGraph.knownEmpty()) {
        if (explain)
           cerr << "static analysis determined that the query result will be empty" << endl; else
           cout << "<empty result>" << endl;
        return 0;
     }

      timer.start();
      // Data mode
      db->setEnv(false);
      PlanGen plangen;
      Plan* plan=plangen.translate(db,queryGraph);
      timer.stop();
      LOG4CXX_INFO(logger, "Plan generated in " << timer);
      LOG4CXX_INFO(logger, "<plan>............................");
      plan->print(0);
      LOG4CXX_INFO(logger, "</plan>............................");
      unsigned id = 1;
      serializePlan(plan,plan_serialize,id);
      if (!plan) {
         cerr << "internal error plan generation failed" << endl;
         return 0;
      }
      return true;
  }
}


bool QueryEngine::planGenerator(string &query, VECTOR_INT_32& plan_serialize, VECTOR_INT_32 &plist, bool mode)
   // Evaluate a query
{

	bool explain = false;

	if (query.substr(0, 8) == "explain " && query.substr(0, 7) == "EXPLAIN "){
		query = query.substr(8);
		explain = true;

	}else if(query.substr(0, 7) != "select " && query.substr(0, 7) != "SELECT "){
		std:cout << "Query:" << query.substr(0,7) << "| Expecting SPARQL query"<< endl;
		return 0;
	}
	if(mode){
		QueryGraph squeryGraph;
		{
			timer.reset();
			// Parse the query
			SPARQLLexer lexer(query);
			SPARQLParser parser(lexer);
			try {
				parser.parse();
			} catch (const SPARQLParser::ParserException& e) {
				LOG4CXX_ERROR(logger, "parse error: " << e.message);
				return 0;
			}
			timer.stop();
			LOG4CXX_INFO(logger, "Query parsed in " << timer);

			// And perform the semantic anaylsis
			timer.start();
			try {
				SemanticAnalysis semana(dict,true);
				semana.transform(parser,squeryGraph);
			} catch (const SemanticAnalysis::SemanticException& e) {
				LOG4CXX_ERROR(logger,"semantic error: " << e.message);
				return 0;
			}
			timer.stop();
			LOG4CXX_INFO(logger, "Semantic analysis in " << timer);
			if (squeryGraph.knownEmpty()) {
				if (explain)
					cerr << "static analysis determined that the query result will be empty" << endl; else
						cout << "<empty result>" << endl;
				return 0;
			}

			//Summary Query execution
			timer.start();
			// Set summary mode!
			db->setEnv(true);
			PlanGen splangen;
			Plan* splan=splangen.translate(db,squeryGraph);
			timer.stop();
			LOG4CXX_INFO(logger, "Plan generated in " << timer);
			LOG4CXX_INFO(logger, "<Summary plan>............................");
			splan->print(0);
			LOG4CXX_INFO(logger, "</Summary plan>............................");
			timer.start();
			Relation *srel = executeQuery(db,splan);
			srel->partitionList(plist,db->statistics->plist);
			timer.stop();
			LOG4CXX_INFO(logger, "Summary query processing in: " << timer);
		}
	}
	QueryGraph queryGraph;
  {
     rg::Timer timer;
     timer.start();
	  // Parse the query
     SPARQLLexer lexer(query);
     SPARQLParser parser(lexer);
     try {
        parser.parse();
     } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        return 0;
     }
     timer.stop();
     LOG4CXX_INFO(logger, "parsed in " << timer);

     // And perform the semantic anaylsis
     timer.start();
     try {
        SemanticAnalysis semana(dict,false);
        semana.transform(parser,queryGraph);
     } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        return 0;
     }
     timer.stop();
     LOG4CXX_INFO(logger, "Semantic analysis in " << timer);
     if (queryGraph.knownEmpty()) {
        if (explain)
           cerr << "static analysis determined that the query result will be empty" << endl; else
           cout << "<empty result>" << endl;
        return 0;
     }

      timer.start();
      // Data mode
      db->setEnv(false);
      vector<unsigned> stats_vector;
      PlanGen plangen;

      plangen.prerequistes(db,queryGraph,stats_vector);

      Plan* plan=plangen.translate(db,queryGraph);
      timer.stop();
      LOG4CXX_INFO(logger, "Plan generated in " << timer);
      LOG4CXX_INFO(logger, "<plan>............................");
      plan->print(0);
      LOG4CXX_INFO(logger, "</plan>............................");
      unsigned id = 1;
      serializePlan(plan,plan_serialize,id);
      if (!plan) {
         cerr << "internal error plan generation failed" << endl;
         return 0;
      }
      return true;
  }
}

Utilities::value32_t QueryEngine::serializePlan(Plan* plan, VECTOR_INT_32 &plan_serialize, unsigned &node_id){
	if(!plan)
		return ~0u;
	else{
		unsigned left_id = 0, right_id = 0;
		if(plan->op == Plan::HashJoin || plan->op == Plan::MergeJoin){
			left_id = serializePlan(plan->left, plan_serialize, node_id);
			right_id = serializePlan(plan->right, plan_serialize, node_id);
		}

		plan_serialize.push_back(node_id);
		plan_serialize.push_back(left_id);
		plan_serialize.push_back(right_id);

		plan_serialize.push_back(plan->op);  			// Operator
		plan_serialize.push_back(plan->opArg);			// Operator argument

		plan_serialize.push_back(plan->cardinality);	// Estimated cardinality
		plan_serialize.push_back(plan->costs);			// Plan costs

		plan_serialize.push_back(plan->width.valueOf()); //Relation width
		plan_serialize.push_back(plan->ordering);		// Plan ordering

		plan_serialize.push_back(plan->node_loc.valueOf()); // Plan execution node
		plan_serialize.push_back(plan->transfer);		// Transfering information

		plan_serialize.push_back(plan->sub);
		//plan_serialize.push_back(plan->subC);
		plan_serialize.push_back(plan->pred);
		//plan_serialize.push_back(plan->predC);
		plan_serialize.push_back(plan->obj);
		//plan_serialize.push_back(plan->objC);

		return node_id++;

	}
}
//---------------------------------------------------------------------------
Plan* QueryEngine::deSerializePlan(VECTOR_INT_32 &plan_serialize, unsigned &it){
	if(it <= 1)
		return 0;
	else{
		Plan* p = plans.alloc();
		//p->objC = plan_serialize.at(--it);
		p->obj = plan_serialize.at(--it);
		//p->predC = plan_serialize.at(--it);
		p->pred = plan_serialize.at(--it);
		//p->subC = plan_serialize.at(--it);
		p->sub = plan_serialize.at(--it);
		p->transfer = (Plan::Transfer)plan_serialize.at(--it);
		p->node_loc = BitSet(plan_serialize.at(--it));
		p->ordering = plan_serialize.at(--it);
		p->width = BitSet(plan_serialize.at(--it));
		p->costs = plan_serialize.at(--it);
		p->cardinality = plan_serialize.at(--it);
		p->opArg = plan_serialize.at(--it);
		p->op = (Plan::Op)plan_serialize.at(--it);
		p->id = plan_serialize.at(it-3);

		unsigned temp = it;
		it = it - 3;
		if(plan_serialize.at(temp-1)){
			p->right = deSerializePlan(plan_serialize, it);
		}else{
			p->right = 0;
		}
		if(plan_serialize.at(temp-2)){
			p->left = deSerializePlan(plan_serialize, it);
		}else{
			p->left = 0;
		}
		return p;
	}
}
// </Intialization and plan generation>
