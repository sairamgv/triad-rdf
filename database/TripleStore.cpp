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

#include "TripleStore.hpp"

TripleStore::TripleStore() {
	// TODO Auto-generated constructor stub
	numTriples = 0;
	for(int i = 0; i < 6; i++)
		dataIndex[i] = new FastMap();
	for(int i = 0; i < 3; i++)
		summaryIndex[i] = new FastMap();
	plist.clear();
}

TripleStore::~TripleStore() {
	// TODO Auto-generated destructor stub
}

LoggerPtr TripleStore::logger(Logger::getLogger("TriAD"));


bool TripleStore::addTriple(Utilities::value32_t subject, Utilities::value32_t predicate,
		Utilities::value32_t object,  unsigned cSize){

	unsigned spart = (subject >> pbits), opart = (object >> pbits);
	unsigned sLoc = spart % cSize, oLoc = opart % cSize;

	tmpTripleVector[sLoc].push_back(0); tmpTripleVector[sLoc].push_back(subject); tmpTripleVector[sLoc].push_back(predicate); tmpTripleVector[sLoc].push_back(object);
	tmpTripleVector[oLoc].push_back(4); tmpTripleVector[oLoc].push_back(object); tmpTripleVector[oLoc].push_back(predicate); tmpTripleVector[oLoc].push_back(subject);

	properties.insert(predicate);

	numTriples++;
	return true;
}

void TripleStore::joinCardinality(vector<unsigned> & jCard, bool summary){

	FastMap *idx2, *idx3;

	if(summary){
		idx2 = summaryIndex[0];   //ps
		idx3 = summaryIndex[1];   //po
	}else{
		idx2 = dataIndex[2];     //ps
		idx3 = dataIndex[3];     //po
	}
	unsigned path_card = 0, star1_card = 0, star2_card = 0;


	for(set<Utilities::value32_t>::iterator p1 = properties.begin(); p1 != properties.end(); p1++)
		for(set<Utilities::value32_t>::iterator p2 = properties.begin(); p2 != properties.end(); p2++){

			std::pair<FastMap::iterator, FastMap::iterator> it_pair_1_so = idx2->find(*p1);
			std::pair<FastMap::iterator, FastMap::iterator> it_pair_1_os = idx3->find(*p1);
			std::pair<FastMap::iterator, FastMap::iterator> it_pair_2_so = idx2->find(*p2);
			std::pair<FastMap::iterator, FastMap::iterator> it_pair_2_os = idx3->find(*p2);

			unsigned path_card = 0, star1_card = 0, star2_card = 0;

			FastMap::iterator t1_so = it_pair_1_so.first;
			FastMap::iterator t1_os = it_pair_1_os.first;
			FastMap::iterator t2_so = it_pair_2_so.first;
			FastMap::iterator t2_os = it_pair_2_os.first;

			// Path cardinality
			while(it_pair_1_os.first != it_pair_1_os.second && it_pair_2_so.first != it_pair_2_so.second){
				if(it_pair_1_os.first.value(2) < it_pair_2_so.first.value(2))
					it_pair_1_os.first++;
				else if(it_pair_1_os.first.value(2) > it_pair_2_so.first.value(2))
					it_pair_2_so.first++;
				else{
					unsigned joinval = it_pair_1_os.first.value(2);
					unsigned c1 = 0, c2 = 0;
					while(it_pair_1_os.first != it_pair_1_os.second && it_pair_1_os.first.value(2) == joinval) {c1++;it_pair_1_os.first++;}
					while(it_pair_2_so.first != it_pair_2_so.second && it_pair_2_so.first.value(2) == joinval) {c2++;it_pair_2_so.first++;}
					path_card += (c1*c2);
				}
			}
			// Star1 cardinality
			it_pair_1_so.first = t1_so;
			it_pair_2_so.first = t2_so;
			while(it_pair_1_so.first != it_pair_1_so.second && it_pair_2_so.first != it_pair_2_so.second){
				if(it_pair_1_so.first.value(2) < it_pair_2_so.first.value(2))
					it_pair_1_so.first++;
				else if(it_pair_1_so.first.value(2) > it_pair_2_so.first.value(2))
					it_pair_2_so.first++;
				else{
					unsigned joinval = it_pair_1_so.first.value(2);
					unsigned c1 = 0, c2 = 0;
					while(it_pair_1_so.first != it_pair_1_so.second && it_pair_1_so.first.value(2) == joinval) {c1++;it_pair_1_so.first++;}
					while(it_pair_2_so.first != it_pair_2_so.second && it_pair_2_so.first.value(2) == joinval) {c2++;it_pair_2_so.first++;}
					star1_card += (c1*c2);
				}
			}
			// Star2 cardinality
			it_pair_1_os.first = t1_os;
			it_pair_2_os.first = t2_os;
			while(it_pair_1_os.first != it_pair_1_os.second && it_pair_2_os.first != it_pair_2_os.second){
				if(it_pair_1_os.first.value(2) < it_pair_2_os.first.value(2))
					it_pair_1_os.first++;
				else if(it_pair_1_os.first.value(2) > it_pair_2_os.first.value(2))
					it_pair_2_os.first++;
				else{
					unsigned joinval = it_pair_1_os.first->val2;
					unsigned c1 = 0, c2 = 0;
					while(it_pair_1_os.first != it_pair_1_os.second && it_pair_1_os.first.value(2) == joinval) {c1++;it_pair_1_os.first++;}
					while(it_pair_2_os.first != it_pair_2_os.second && it_pair_2_os.first.value(2) == joinval) {c2++;it_pair_2_os.first++;}
					star2_card += (c1*c2);
				}
			}
			jCard.push_back(*p1);
			jCard.push_back(*p2);
			jCard.push_back(path_card);
			jCard.push_back(star1_card);
			jCard.push_back(star2_card);
		}

}

bool TripleStore::add(VECTOR_INT_32 & vect){

	for(int i = 0; i < 6; i++)
		dataIndex[i]->pbits = pbits;



	for(VECTOR_INT_32::iterator it = vect.begin(); it != vect.end();){
		unsigned type = *(it++), val1 = *(it++);
		unsigned val2 = *(it++), val3 = *(it++);
		if(type == 0){ // S, P, O
			dataIndex[0]->insert(val1,val2,val3); // SPO
			dataIndex[1]->insert(val1,val3,val2); // SOP
			dataIndex[2]->insert(val2,val1,val3); // PSO
			if(pbits){	// Construct summaries only when pbits > 0
				summaryIndex[0]->insert(val2,(val1>>pbits),(val3>>pbits));
	
				//std::cout << "pbits.." << pbits << " adding summary triple.. " << val2 << " " << val1 << "("<< (val1>>pbits)<< ") " << val3 << "(" << (val3>>pbits) << ")" << endl;
			}
		}else if(type == 4){ // O, S , P
			dataIndex[3]->insert(val2,val1,val3); // POS
			dataIndex[4]->insert(val1,val2,val3); // OPS
			dataIndex[5]->insert(val1,val3,val2); // OSP
		}
		properties.insert(val2);
	}
	return true;
}

void TripleStore::computeCardinalities(FastMap *idx, vector<unsigned> & jCard){

	unsigned card = 0, card1 = 0;
	FastMap::iterator it = idx->begin();
	for(;it != idx->end(); it++){

		if(it == idx->begin()){
			jCard.push_back(it.value(1));
		}
		else if((it-1).value(1) != it.value(1) ){
			jCard.push_back((it-1).value(2));
			jCard.push_back(card1);
			jCard.push_back(~0u);
			jCard.push_back(card);
			jCard.push_back(it.value(1));
			card = 0; card1 = 0;
		}else if((it-1).value(2) != it.value(2)){
			jCard.push_back((it-1).value(2));
			jCard.push_back(card1);
			card1 = 0;
		}
		card++; card1++;
	}
	jCard.push_back((it-1).value(2));
	jCard.push_back(card1);
	jCard.push_back(~0u);
	jCard.push_back(card);
}


void TripleStore::serializeSummary(vector<unsigned> & vect){
	for(FastMap::iterator it = summaryIndex[0]->begin(); it != summaryIndex[0]->end(); it++){
		vect.push_back(it.value(1)); vect.push_back(it.value(2)); vect.push_back(it.value(3));
	}
	delete summaryIndex[0];
}

void TripleStore::computeCardinalities(unsigned type, vector<unsigned> & jCard, bool summary){
	(summary)?
	computeCardinalities(summaryIndex[type],jCard):
	computeCardinalities(dataIndex[type],jCard);
}

void TripleStore::sort(){

	for(int type = 0; type < 6; type++)
		dataIndex[type]->optimize(0);

	for(int type = 0; type < 3; type++)
		summaryIndex[type]->optimize(0);
}

void TripleStore::stats(){
	for(int type = 0; type < 6; type++){
		LOG4CXX_INFO(logger, dataIndex[type]->size());
	}

	for(int type = 0; type < 3; type++){
		LOG4CXX_INFO(logger, summaryIndex[type]->size());
	}
}

//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
// Distributed functions (each node executes concurrently)
void TripleStore::computeQueryStatistics(vector<unsigned> & stats_req, vector<unsigned> & stats){
	unsigned order, val1, val2, card;
	pair<FastMap::iterator, FastMap::iterator> iter;
	for(vector<unsigned>::iterator it = stats_req.begin(); it != stats_req.end(); it++){
		if(*it == 0){ // type: single cardinality
			order = *(++it);
			val1 = *(++it);
			iter = dataIndex[order]->find(val1);
			stats.push_back(iter.second-iter.first);
			//std::cout << 0 << "::"<< order << " " << val1 << " card:" << (iter.second-iter.first) << endl;
		}
		if(*it == 1){ // type: pair cardinality
			order = *(++it);
			val1 = *(++it);
			val2 = *(++it);
			iter = dataIndex[order]->find(val1,val2);
			stats.push_back(iter.second-iter.first);
			//std::cout << 1 << "::"<< order << " " << val1<< " " << val2 << " card:"<< (iter.second-iter.first) << endl;
		}
	}

}
