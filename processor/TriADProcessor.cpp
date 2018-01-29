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

#include "TriADProcessor.hpp"

using namespace std;
using namespace mpi2;
using namespace rg;


TriADProcessor::TriADProcessor() {
	// TODO Auto-generated constructor stub
	db = new Database();
	qEngine = new QueryEngine();
}

TriADProcessor::~TriADProcessor() {
	// TODO Auto-generated destructor stub
}

LoggerPtr TriADProcessor::logger(Logger::getLogger("TriAD"));

bool TriADProcessor::buildPerNodeDatabase(Channel ch) {

	Env & env = mpi2::env();

	TaskManager& tm = TaskManager::getInstance();
	boost::mpi::communicator& world = tm.world();

	unsigned rank = world.rank();

	bool myTurn = false;
	while (true) {
		ch.recv(myTurn);
		if (!myTurn)
			break;
		else {
			Utilities::value32_t key, type;
			vector<Utilities::value32_t> vect;
			ch.recv(vect);
			if(!db->tripleIndex->add(vect))
				return false;
		}
	}
	LOG4CXX_INFO(logger, "Node-" << rank << ": Sorting triples...");
	db->tripleIndex->sort();
	LOG4CXX_INFO(logger, "Node-" << rank << ": Computing statistics...");
	vector<unsigned> jCard;
	db->tripleIndex->joinCardinality(jCard,false);
	//LOG4CXX_INFO(logger, "Node-" << rank << ": jCard Size: " << jCard);
	ch.send(jCard);

	unsigned size;
	// S-P cardinality
/*
	jCard.clear();
	db->tripleIndex->computeCardinalities(0,jCard,false);
	LOG4CXX_INFO(logger, "Node-" << rank << ": computing cardinalities...");
	size = jCard.size();
	ch.send(size);
	for(unsigned i = 0; i < size;){
		vector<unsigned> vect;
		if(i+10000000> size)
			vect.insert(vect.begin(), jCard.begin()+i, jCard.end());
		else
			vect.insert(vect.begin(), jCard.begin()+i, jCard.begin()+i+10000000);
		i = i + 10000000;
		ch.send(vect);
	}
	// P-O cardinality
	jCard.clear();
	db->tripleIndex->computeCardinalities(3,jCard,false);
	size = jCard.size();
	ch.send(size);

	for(unsigned i = 0; i < size;){
		vector<unsigned> vect;
		if(i+10000000 > size)
			vect.insert(vect.begin(), jCard.begin()+i, jCard.end());
		else
			vect.insert(vect.begin(), jCard.begin()+i, jCard.begin()+i+10000000);
		i = i + 10000000;
		ch.send(vect);
	}
	// O-S cardinality
	jCard.clear();
	db->tripleIndex->computeCardinalities(5,jCard,false);
	size = jCard.size();
	ch.send(size);

	for(unsigned i = 0; i < size;){
		vector<unsigned> vect;
		if(i+10000000 > size)
			vect.insert(vect.begin(), jCard.begin()+i, jCard.end());
		else
			vect.insert(vect.begin(), jCard.begin()+i, jCard.begin()+i+10000000);
		i = i + 10000000;
		ch.send(vect);
	}
*/

	///Sending summary information
	if(db->tripleIndex->pbits){
		LOG4CXX_INFO(logger, "Node-" << rank << ": Sending local summaries...")
		vector<unsigned> sumVect;
		db->tripleIndex->serializeSummary(sumVect);
		size = sumVect.size();
		ch.send(size);
		for(unsigned i = 0; i < size;){
			vector<unsigned> vect;
			if(i+10000000 > size)
				vect.insert(vect.begin(), sumVect.begin()+i, sumVect.end());
			else
				vect.insert(vect.begin(), sumVect.begin()+i, sumVect.begin()+i+10000000);
			i = i + 10000000;
			ch.send(vect);
		}
	}
	return true;
}


bool TriADProcessor::executePerNodeQuery(Channel ch, vector<Channel> & channels, vector<unsigned> & results, unsigned& numVars) {

	Env & env = mpi2::env();

	TaskManager& tm = TaskManager::getInstance();
	boost::mpi::communicator& world = tm.world();
	unsigned rank = world.rank();

	{// for per query stats
		vector<unsigned> stats_req, stats;
		ch.recv(stats_req);
		db->tripleIndex->computeQueryStatistics(stats_req,stats);
		ch.send(stats);
	}


	Plan* plan = 0;
	{
		vector<Utilities::value32_t> query_serialized;
		ch.recv(query_serialized);
		db->tripleIndex->plist.clear();
		ch.recv(db->tripleIndex->plist);
		unsigned len = query_serialized.size();
		plan = qEngine->deSerializePlan(query_serialized, len);
	}

	LOG4CXX_DEBUG(logger, "Done receving query at " << rank << " from " << ch.remote());

	if(plan){
		qEngine->timer.start();
		Relation* output = qEngine->executeQuery(db,plan,channels,rank,true);
		qEngine->timer.stop();
		numVars = output->width;
		results = output->tuples;
		if(output){
				LOG4CXX_INFO(logger, "Node-" << rank << ": Final Result size: " << output->numTuples);
		}
		else{
			LOG4CXX_INFO(logger, "Node-" << rank << ": Final Result size: <empty> ");}

		//LOG4CXX_INFO(logger, "Node-" << rank << ": Processed query in time (s) " << qEngine->timer);
	}

	return true;

}


void TriADProcessor::waitForCompletion(vector<Channel> & channels){
	Env & env = mpi2::env();

	TaskManager& tm = TaskManager::getInstance();
	boost::mpi::communicator& world = tm.world();
	unsigned rank = world.rank();

	std::vector<boost::mpi::request> reqs, reqs_2;

	for(int i = 1; i < channels.size(); i++){
			bool finish;
			reqs.push_back(channels[i].irecv(finish));
	}
	// Modified economic wait all
	int unfinished = reqs.size();
	bool flag;
	while (unfinished > 0) {
		flag = false;
		for (unsigned i = 0; i < reqs.size(); i++) {
			boost::optional < boost::mpi::status > msg = reqs[i].test();
			if (msg && (status & (1<<i+1))) {
				unfinished--;
				status &= ~(1 << i+1);
				flag = true;
				//query_time.stop();
				//setCopyAll("status", status);
				//query_time.resume();
			}
		}
		if(flag)
		for (unsigned i = 0; i < reqs.size(); i++){
		 	boost::optional < boost::mpi::status > msg = reqs[i].test();
			if (!msg && (status & (1<<i+1))) {
				reqs_2.push_back(isetCopy(i+1,"status", status));
			}
		}

		boost::mpi::wait_all(reqs_2.begin(),reqs_2.end());
		reqs_2.clear();
		// Lazy probe?
		if (reqs.size() > 0) {
		boost::this_thread::sleep(boost::posix_time::microsec(5));
		}
	}
	LOG4CXX_INFO(logger, "finished waiting @master");
	return;
}


static bool readLine(string& query)
   // Read a single line
{
#ifdef CONFIG_LINEEDITOR
   // Use the lineeditor interface
   static lineeditor::LineInput editHistory(L">");
   return editHistory.readUtf8(query);
#else
   // Default fallback
   cerr << "Enter your query or use \"quit\">"; cerr.flush();
   return getline(cin,query);
#endif
}

static string readInput(istream& in)
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

static void distributePlan(vector<unsigned> &plan_vector,vector<unsigned> &plan_plist, CHANNEL &channels){
	vector<unsigned> vect;
	for(int i = 1; i < channels.size(); i++){
		channels[i].send(plan_vector);
		channels[i].send(plan_plist);
	}
}


void TriADProcessor::parseArgument(int argc, char* args[]){

	LOG4CXX_INFO(logger, "Parsing arguments...");

	silent = false; // Print results by default
	pbits = 0;
	for (int i = 1; i < argc; i++) {
		if (!std::strcmp(args[i], "-i")) {
			dataFile = args[i + 1];
		} else if (!std::strcmp(args[i], "-p")) {
			partitionFile = args[i + 1];
		} else if (!std::strcmp(args[i], "-d")) {
			dictionaryFile = args[i + 1];
		} else if (!std::strcmp(args[i], "-q")) {
			queryFile = args[i + 1];
		} else if (!std::strcmp(args[i], "-e")) {
			encodedFile = args[i + 1];
		} else if (!std::strcmp(args[i], "-parts")) {
			pbits = atoi(args[i + 1]);
		} else if (!std::strcmp(args[i], "-s")) {
			silent = true;
		}
	}
	if(pbits){
		db->numPartitions = pbits;
		pbits = 32 - log(pbits+100) / log(2);
	}
	LOG4CXX_INFO(logger, "" << "Input Path/File... " << dataFile);
	LOG4CXX_INFO(logger, "" << "Partition File... " << partitionFile);
	LOG4CXX_INFO(logger, "" << "Dictionary File... " << dictionaryFile);
	LOG4CXX_INFO(logger, "" << "Encoded File... " << encodedFile);
	LOG4CXX_INFO(logger, "" << "Query File... " << queryFile);
	LOG4CXX_INFO(logger, "" << "Print Results... " << ((silent)? "No":"Yes"));
	LOG4CXX_INFO(logger, "" << "Partition bits... " << pbits);

}

void TriADProcessor::startIndexTask(vector<Channel> channels){

	db->set_pbits(pbits);
	if(silent)
		db->dictionary->skipStrings = true;

	for (int i = 1; i < channels.size(); i++){
		channels[i].send(pbits);
		channels[i].send(silent);
	}

	if(pbits){
		LOG4CXX_INFO(logger, "Setting summary mode...");
		LOG4CXX_INFO(logger, "Loading summary information...");
		db->loadSummary(partitionFile, dictionaryFile);
	}else
		LOG4CXX_INFO(logger, "Setting non-summary (basic) mode...");

	LOG4CXX_INFO(logger, "Loading triples...");
	//db->build(dataFile, channels);
	{	// custom dictionaries
		//db->loadDictionary(dictionaryFile);
		//std::cout << "reading triples.." << endl;
		//db->buildWithPreEncoding(encodedFile,channels);
		db->build(dataFile, channels);
	}
	
}

static void receive_stats_from_slaves(Database *db, vector<unsigned> &vect, vector<Channel> & channels){

	// communicate stat requests to slaves
	for (int i = 1; i < channels.size(); i++){
		channels[i].send(vect);
	}

	vector<unsigned> stats[channels.size()];
	// receive stats;
	for (int i = 1; i < channels.size(); i++){
		channels[i].recv(stats[i]);
	}

	db->statistics->dynamicStats(vect,stats, channels.size());
}

void TriADProcessor::startQueryTask(vector<Channel> channels) {
	status = ~0u;
	createCopyAll("status", status);
	//lsAll();  // DEBUG

	bool nextQuery;
	qEngine->init(db);

	//rg::Timer query_time;

	while (true) {
		string query;
		if (!readLine(query) || query == "quit" || query == "\\q")
			break;
		query_time.start();
		rg::Timer tmp;
		
		nextQuery = true;
		vector<unsigned> plan_serialzied;
		vector<unsigned> plan_plist;
		vector<unsigned> plan_reqvect;
		bool summary = (pbits)? true : false;
		bool ret = true;
		tmp.start();
		ret = qEngine->buildStatRequest(query,plan_reqvect);

		if(!ret) continue;

		for (int i = 1; i < channels.size(); i++)
			channels[i].send(nextQuery);

//		rg::Timer t1;
//		t1.start();
		{
			receive_stats_from_slaves(db,plan_reqvect, channels);
		}
//		t1.stop();
		tmp.stop();
		LOG4CXX_INFO(logger, "Statistics overhead time " << tmp);

		tmp.start();
		ret = qEngine->planGenerator(query, plan_serialzied, plan_plist,summary);
		tmp.stop();
		LOG4CXX_INFO(logger, "Plan generation time " << tmp);

		if (ret) {
			tmp.start();
			distributePlan(plan_serialzied, plan_plist, channels);
			waitForCompletion(channels);
			tmp.stop();
			LOG4CXX_INFO(logger, "Slaves query processing time " << tmp);
			vector<unsigned> results;
			unsigned width;
			if(!silent){
				for (int i = 1; i < channels.size(); i++){
					vector<unsigned> parts;
					channels[i].recv(width);
					channels[i].recv(parts);
					results.insert(results.end(),parts.begin(),parts.end());
				}
				//std::cout << "Results size " << results.size()/width << endl;
				qEngine->printResults(results,width);
			}

			query_time.stop();

			status = ~0u;
			setCopyAll("status", status);

		}
		//query_time.stop();
		LOG4CXX_INFO(logger, "Total query time:" << query_time);
	}
	nextQuery = false;
	for (int i = 1; i < channels.size(); i++)
		channels[i].send(nextQuery);
}
