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
#include "Database.hpp"

using namespace std;

Database::Database() {
	clusterSize = 2;
	dictionary = new Dictionary();
	tripleIndex = new TripleStore();
	statistics = new Statistics();
}

Database::~Database() {
	// TODO Auto-generated destructor stub
}

LoggerPtr Database::logger(Logger::getLogger("TriAD"));

bool Database::processTriple(string sub, string pred, string obj) {

	unsigned subIdx, predIdx, objIdx;
	unsigned sPartId = ~0u, pPartId = ~0u, oPartId = ~0u;


	bool ret_dict = (dictionary->insert(sub, subIdx, sPartId, sPartId, get_pbits())
			&& dictionary->insert(pred, predIdx, pPartId, pPartId, get_pbits())
			&& dictionary->insert(obj, objIdx, oPartId, sPartId, get_pbits()));
	if (!ret_dict)
		std::cout << "Dict Failed" << endl;		//DEBUG
	// std::cout << sPartId << " " << oPartId << endl;

	if(tripleIndex->pbits){
		if(!~oPartId) oPartId = sPartId;
		if(!~oPartId)
			std::cout << "Sub and Obj don't have ids " << sub << " " << pred << " " << obj << endl;
	}
	//if(!~oPartId)
	//std::cout << subIdx << "(" << (subIdx >> get_pbits())  << ")  " << predIdx << " " << objIdx << "(" << (objIdx >> get_pbits())  << ")" << endl;

	
	bool ret_ti =tripleIndex->addTriple(subIdx, predIdx, objIdx, clusterSize); // triple;

	if (!ret_ti){
		std::cout << "Triple addition failed" << endl;		//DEBUG
		return false;
	}
	statistics->updateTotalCardnality();
	return true;
}

void Database::computeSummaryStatistics(){

	unsigned all_path_card =0, all_star1_card = 0, all_star2_card = 0;
	// <Obtain summary index from slaves>
	// Choice: (i) Create summary at master itself while parsing data [con: requires large memory!]
	//        (ii) Obtain summary from slaves [con: extra back communication of triples]
	 LOG4CXX_INFO(logger, "Receiving summary...");

	for (int i = 0; i < clusterSize; i++) {
		vector<unsigned> sumVect;
		unsigned size;
		channels[i + 1].recv(size);
		vector<unsigned> vect;
		for (unsigned j = 0; j < size;) {
			vect.clear();
			channels[i + 1].recv(vect);
			j = j + vect.size();
			sumVect.insert(sumVect.end(), vect.begin(), vect.end());
		}
		unsigned p,sp,op;
		for(int j = 0; j < sumVect.size();){
			p = sumVect.at(j);
			sp = sumVect.at(j+1);
			op = sumVect.at(j+2);
			j += 3;
			//std::cout << p << " " << sp << " " << op << endl;
			tripleIndex->summaryIndex[0]->insert(p,sp,op);
			tripleIndex->summaryIndex[1]->insert(p,op,sp);
		}
	}
	tripleIndex->summaryIndex[0]->optimize(0);
	tripleIndex->summaryIndex[1]->optimize(0);
	//tripleIndex->summaryIndex[0]->print();
	

	//LOG4CXX_INFO(logger, "Summary Index size " << tripleIndex->summaryIndex[0]->size()); //DEBUG
	///Summary statistics
	
	LOG4CXX_INFO(logger,"Computing summary statistics...");

	vector<Utilities::value32_t> vect;
    tripleIndex->joinCardinality(vect,true);
    all_path_card = 0; all_star1_card = 0; all_star2_card = 0;

    for (vector<Utilities::value32_t>::iterator it = vect.begin();
 		   it != vect.end();) {
 	   Utilities::value32_t p1 = (*it++);
 	   Utilities::value32_t p2 = (*it++);
 	   Utilities::value32_t path_card = (*it++);
 	   Utilities::value32_t star1_card = (*it++);
 	   Utilities::value32_t star2_card = (*it++);
 	   all_path_card += path_card;
 	   all_star1_card += star1_card;
 	   all_star2_card += star2_card;
 	   statistics->updateJoinSelectivity(p1, p2, path_card, star1_card,
 			   star2_card,true);
    }
    for(int type = 0; type < 2; type++){
    	vect.clear();
    	tripleIndex->computeCardinalities(type,vect,true);
    	unsigned val1=~0u, val2=0,card=0;
    	for(vector<Utilities::value32_t>::iterator it = vect.begin(); it != vect.end();){
    			if(!~val1)
    				val1 = *it++;
    			val2 = *it++;
    			card = *it++;
    			if(!~val2){
    				if(type == 1){ // Only predicates, once is enough
    					//std::cout << val1 << " " << card << endl;
    					statistics->updateCardinality1(type,val1,card,true);
    				}
                    if(it != vect.end())
                    	val1 = *it++;
    			}else{
    				//std::cout << val1 << " " << val2 << " " << card << endl;
    				statistics->updateCardinality1(type,val1,val2, card,true);
    			}
    	}
    }

}

void Database::computeDataStatistics(){

	unsigned all_path_card =0, all_star1_card = 0, all_star2_card = 0;

	LOG4CXX_INFO(logger, "Receiving statistics...");

	// Get the join selectivities from slaves
	for(int i = 0; i < clusterSize; i++){
		vector<unsigned> jsVect; jsVect.clear();
		channels[i+1].recv(jsVect);
		for(vector<unsigned>::iterator it = jsVect.begin(); it != jsVect.end();){
			Utilities::value32_t p1 = (*it++);
			Utilities::value32_t p2 = (*it++);
			Utilities::value64_t path_card = (*it++);
			Utilities::value64_t star1_card = (*it++);
			Utilities::value64_t star2_card = (*it++);
			all_path_card += path_card;
			all_star1_card += star1_card;
			all_star2_card += star2_card;
			statistics->updateJoinSelectivity(p1, p2, path_card,star1_card, star2_card,false);
		}
	}
    /// Cardinalities
    /*for(unsigned type = 0; type < 3; type++)
    for (int i = 0; i < clusterSize; i++) {
		vector<unsigned> jsVect;
		unsigned size;
		channels[i + 1].recv(size);
		vector<unsigned> vect;
		// Receive the data in chunks
		for (unsigned j = 0; j < size;) {
			vect.clear();
			channels[i + 1].recv(vect);
			j = j + vect.size();
			jsVect.insert(jsVect.end(), vect.begin(), vect.end());
		}
		unsigned val1 = ~0u, val2 = 0, card = 0;
		for (vector<unsigned>::iterator it = jsVect.begin(); it != jsVect.end();) {
			if (!~val1)
				val1 = *it++;
			val2 = *it++;
			card = *it++;
			if (!~val2) {
				statistics->updateCardinality1(type, val1, card,false);
				if (it != jsVect.end())	val1 = *it++;
			} else {
				statistics->updateCardinality1(type, val1, val2, card,false);
			}
		}
	}*/


}

void Database::build(string path, CHANNEL &channels) {
	clusterSize = channels.size() - 1;
	tripleIndex->clusterSize = clusterSize;
	this->channels = channels;
	rg::Timer timer;
	readTriples(path);
	dictionary->optimize(true);
	tripleIndex->sort();
	computeDataStatistics();
	if(this->get_pbits())
		computeSummaryStatistics();
	statistics->optimize();
}

void Database::loadData(string dataFile) {

	ifstream dataIn(dataFile.c_str());
	string triple; unsigned count = 0;

	while (dataIn.good() && getline(dataIn,triple)) {
	  unsigned val1, val2, val3;
 	  istringstream iss(triple);
	  iss >> val1 >> val2 >> val3;
 	  tripleIndex->addTriple(val1,val2,val3,clusterSize);
	  if(count > 500000000) break;
	  if(++count % MAX_TUPLES == 0){
	    std::cout << "Triples indexed: " << count << endl;
	    distributeIndex(true);      
	  }       
	}
    distributeIndex(false);
    LOG4CXX_DEBUG(logger, "Loaded Data");
}

void Database::loadSummary(string partFile, string dictFile) {

	ifstream partIn(partFile.c_str());
	ifstream dictIn(dictFile.c_str());

	string text, part;
	getline(dictIn, text);
	getline(partIn, part);
	while (dictIn.good() && partIn.good()) {
		unsigned p = atoi(part.c_str());
		dictionary->addPartInfo(text,p+100,get_pbits());
		getline(dictIn, text);
		getline(partIn, part);
	}
	dictionary->optimize(false); //optimize but don't delete std::dictionary
}

void Database::distributeIndex(bool flag){
	LOG4CXX_INFO(logger, "Initiating partial distribution...");
	bool turn = true;
	for(int i = 0; i < clusterSize; i++){
		channels[i+1].send(turn);
		channels[i+1].send(tripleIndex->tmpTripleVector[i]);
		tripleIndex->tmpTripleVector[i].clear();
		if(!flag)
			channels[i+1].send(flag);
	}
}

void Database::readTriples(string path) {

	rg::Timer timer;
	timer.start();

	unsigned count = 0, countfile = 0;

	DIR *dir;
	struct dirent *ent;

	bool isFile = false;
	// Check whether the path is a file or directory
	string ext = path.substr(path.length()-2);
	if(ext == "n3" || ext == "nt") isFile = true;

	if(isFile || (dir = opendir(path.c_str())) != NULL){
		while (isFile || (ent = readdir (dir)) != NULL) {
			string file = (isFile)? path: (ent->d_name);
			if(file.length() > 3){
				string ext = file.substr(file.length()-2);
				if(ext == "n3" || ext == "nt"){
					string str;
					str.append(path);
					if(!isFile){
						str.append("/"); str.append(file);
					}
					countfile++;
					//std::cout << countfile << " " << str << endl;
					ifstream inputStream(str.c_str());
					Parser parser(inputStream);
					try {

						string subject, predicate, object, objectSubType;
						Type::ID objectType;

						while (true) {
							if (count && count % MAX_TUPLES == 0){
								LOG4CXX_INFO(logger, "Triples Indexed (so far): " << count << " Files read: " << countfile);
								distributeIndex(true);
							}

							try {
								if (!parser.parse(subject, predicate, object, objectType,
										objectSubType))
									break;
								if (!(processTriple(subject, predicate, object) && ++count)){
									LOG4CXX_ERROR(logger, "Error parsing triple.." <<std::cout << subject << " " << predicate << " " << object);
									break;
								}

							} catch (const Parser::Exception& e) {
								LOG4CXX_ERROR(logger, "Parse Exception " << e.message);
								// recover...
								while (inputStream.get() != '\n')
									;
								continue;
							}
						}
					} catch (const Parser::Exception& e) {
						LOG4CXX_ERROR(logger, "Parse Exception " << e.message);
						return;
					}
				}
				if(isFile) break;
			}
		 }
		distributeIndex(false);
	}
	timer.stop();
	LOG4CXX_INFO(logger, "Parsed " << count << " in " << timer);

}

