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

#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#define TURN_ON_EARLY_TAPPING false
#define MAX_TUPLES 10000000

#include <iostream>
#include <istream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <string>

#include <parser/Parser.hpp>
#include "Dictionary.hpp"
#include "TripleStore.hpp"
#include "Statistics.hpp"

#include <mpi2/mpi2.h>
#include <util/evaluation.h>
#include <dirent.h>

using namespace std;
using namespace mpi2;
using namespace rg;
using namespace log4cxx;

typedef std::vector<Channel> CHANNEL;
//#define MAX_TUPLES 1000000

class Database {

public:
	static log4cxx::LoggerPtr logger;

	enum DataOrder {
		Order_Subject_Predicate_Object = 0,
		Order_Subject_Object_Predicate,
		Order_Predicate_Subject_Object,
		Order_Predicate_Object_Subject,
		Order_Object_Predicate_Subject,
		Order_Object_Subject_Predicate,
	};

	Dictionary *dictionary;
	TripleStore *tripleIndex;
	Statistics *statistics;
	CHANNEL channels;

	unsigned clusterSize;
	unsigned numPartitions;

	Database();
	virtual ~Database();

	bool getEnv(){return statistics->getEnv();}

	bool setEnv(bool env){statistics->senEnv(env);}

	void set_pbits(unsigned pbits){tripleIndex->set_pbits(pbits);statistics->set_pbits(pbits);}

	unsigned get_pbits(){return tripleIndex->pbits;}

	// Takes an input stream on the .N3 file and parses it
	void buildIndex(istream& in, unsigned cSize, CHANNEL & channels);

	void buildIndex(string path, unsigned cSize, CHANNEL &channels);

	void build(string path, CHANNEL &channels);


	void loadSummary(string partFile, string dictFile);

	void loadDictionary(string dictfile);

	void buildWithPreEncoding(string path, CHANNEL &channels);

	void readEncodedTriples(string encFile);

	void distributeIndex(bool flag);

	void clean();

	void readTriples(istream& in);

	void readTriplesLUBM(string path);
 
	void loadData(string path);

	void readTriples(string path);

	void computeMasterJoinSelectivities(unsigned & all_path_card, unsigned & all_star1_card, unsigned & all_star2_card);

	void computeDataStatistics();

	void computeSummaryStatistics();

	bool processTriple(string subject, string predicate, string object);

	void loadPartitionMap(istream& partIn, istream& dictIn);

	Statistics* getStatistics() {
		return statistics;
	}

	Dictionary* getDictionary() {
		return dictionary;
	}

	TripleStore* getTripleIndex() {
		return tripleIndex;
	}
};

#endif /* DATABASE_HPP_ */
