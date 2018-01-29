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

#ifndef QUERYENGINE_HPP_
#define QUERYENGINE_HPP_

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <sys/time.h>
#include <fstream>
#include <boost/thread.hpp>

#include "Operator.hpp"
#include "Relation.hpp"
#include <database/Database.hpp>

using namespace std;
using namespace mpi2;
using namespace rg;
using namespace log4cxx;

class QueryEngine {

  private:
	Database *db;
	Dictionary* dict;
	string queryFile;

  public:
	static log4cxx::LoggerPtr logger;
	rg::Timer timer;
	PlanContainer plans;
    QueryEngine();
    virtual ~QueryEngine();

    bool smallAddressSpace();

    string readInput(istream& in);

    bool readLine(string& query);

    void showHelp();

    bool planGenerator(string &query, VECTOR_INT_32& plan_serialize, VECTOR_INT_32& plan_plist, bool mode);

    void runQuery();

    void init(Database* db);

    unsigned serializePlan(Plan* plan, VECTOR_INT_32 &plan_serialize, unsigned &node_id);

    Plan* deSerializePlan(VECTOR_INT_32 &plan_serialize, unsigned &iterator);

    Relation* executeQuery(Database* db, Plan* plan);

    void generateSPhysicalPlan(Database *db, Plan* plan, vector<Relation*> & splan, unsigned joinpos);

    Relation* executeQuery(Database* db, Plan* plan, vector<Channel> &channels, unsigned rank, bool full);

    Relation* executeQuery(Database *db, Plan* plan, Relation* srel, BitSet *width);

    void createThread(boost::promise<Relation*> & ret, Database* db, Plan* plan, vector<Channel> &channels, unsigned rank);

    void shipRelation(Relation* rel, vector<Channel> &channels, unsigned rank, unsigned jpos, unsigned loc, unsigned nodeid);

    void printResults(vector<unsigned> &results, unsigned width);


    // new methods
    bool summaryStep(string &query, VECTOR_INT_32 &plist);

    bool buildStatRequest(string &query, VECTOR_INT_32 &reqVect);

    bool PlanGenerator(string &query, VECTOR_INT_32& plan_serialize);

};

#endif /* QUERYENGINE_HPP_ */
