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

#include <iostream>
#include <fstream>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <boost/unordered_map.hpp>
#include <database/Database.hpp>
#include <queryengine/QueryEngine.hpp>

#ifndef TRIADPROCESSOR_H_
#define TRIADPROCESSOR_H_

using namespace std;
using namespace rg;
using namespace mpi2;

class TriADProcessor {

private:
	unsigned status;
	string dataFile;
	string partitionFile;
	string dictionaryFile;
	string encodedFile;
	string queryFile;
	bool silent;
	rg::Timer query_time;

public:
	static log4cxx::LoggerPtr logger;
	unsigned pbits;

	Database *db;
	QueryEngine *qEngine;

	TriADProcessor();
	virtual ~TriADProcessor();

	void parseArgument(int argc, char* args[]);

	void startIndexTask(vector<Channel> channels);

	void startQueryTask(vector<Channel> channels);

	bool buildPerNodeDatabase(Channel ch);

	bool executePerNodeQuery(Channel ch, vector<Channel> & channels, vector<unsigned> & results, unsigned & numVars);

	void waitForCompletion(vector<Channel> & channels);


};

#endif /* TRIADPROCESSOR_H_ */

