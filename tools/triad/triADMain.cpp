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

#include <processor/TriADProcessor.hpp>
#include <boost/dynamic_bitset.hpp>
using namespace std;
using namespace rg;
using namespace mpi2;

struct TriADMain {
	inline static string id() {
		return "_TRIAD_MAIN_";
	}

	inline static void run(Channel ch, TaskInfo info) {
		TaskManager& tm = TaskManager::getInstance();
		boost::mpi::communicator& world = tm.world();

		unsigned rank = world.rank();

		if(rank == 0)
			return;

		TriADProcessor *localProcessor = new TriADProcessor();
		vector<Channel> channels = info.pairwiseChannels();
		unsigned pbits; bool silent;
		ch.recv(pbits);
		ch.recv(silent);
		localProcessor->db->set_pbits(pbits);
		// % DEBUG
		// LOG4CXX_INFO(TriADProcessor::logger, "pbits: " << pbits << " at rank " << rank);

		//Build per node triple index
		if(world.rank() != 0)
			localProcessor->buildPerNodeDatabase(ch);

		//Listen and process queries
		bool nextQuery = false;
		vector<unsigned> results;
		unsigned numVars;
		while (true) {
			ch.recv(nextQuery);
			if (!nextQuery) {
				break;
			}

			//results = 0;
			localProcessor->executePerNodeQuery(ch,channels,results,numVars);

			// Send "finish" to master
			ch.send(true);
			
			// Send "results"
			if(!silent){
				std::cout << "Results: " << results.size() << " width " << numVars << endl;
				ch.send(numVars);
				ch.send(results);
			}

		}
		return;

	} // END of RUN
};


int main(int argc, char* argv[]){

	// initialize mpi2
		boost::mpi::communicator& world = mpi2init(argc, argv);

	// environment variable
		Env & env = mpi2::env();

	// initialize the channels needed for communication with the nodes
		CHANNEL channels;

	// register AddTask (this is required to be able to actually run it!)
		TaskManager& tm = TaskManager::getInstance();

	registerTask<TriADMain>();

	// fire up task managers (this blocks on all but the root node)

		mpi2start();
		boost::this_thread::sleep(boost::posix_time::milliseconds(100)); // just to ensure output in right order


	if (world.rank() == 0) {

		if (argc < 2) {
			LOG4CXX_ERROR(TriADProcessor::logger,
                                        "Usage Basic Mode..... " << argv[0] << " [location of folder containing .n3/.nt files]");
			LOG4CXX_ERROR(TriADProcessor::logger,
					"Usage Summary Mode..... " << argv[0] << " [location of folder containing .n3/.nt files] [.part.* file] [.g_dict file]");
		} else {

			// start the task managers on the nodes
			tm.spawnAll<TriADMain>(1, channels, true);

			//LOG4CXX_DEBUG(TriADProcessor::logger, "Test..");

			TriADProcessor *masterProcessor = new TriADProcessor();

			masterProcessor->parseArgument(argc, argv);

			masterProcessor->startIndexTask(channels);

			masterProcessor->startQueryTask(channels);
		}
	}

	// shut down task managers
		mpi2stop();

	// shut down mpi2 and mpi
		mpi2finalize();

	return 1;



}
