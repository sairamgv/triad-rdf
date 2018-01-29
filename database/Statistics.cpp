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

#include "Statistics.hpp"

Statistics::Statistics() {
	// TODO Auto-generated constructor stub
	join_selectivity_map.clear();
	totalCardinality = 0;

	c_map[0] = new FastMap();
	c_map[1] = new FastMap();
	c_map[2] = new FastMap();
	pc_map[0] = new FastMap();
	pc_map[1] = new FastMap();
	pc_map[2] = new FastMap();
	//js_map = new FastMap();
	sc_map[0] = new FastMap();
	sc_map[1] = new FastMap();
	sc_map[2] = new FastMap();
	spc_map[0] = new FastMap();
	spc_map[1] = new FastMap();

	cache_sc = new FastMap();
	cache_pc = new FastMap();

	summary = false;

}

Statistics::~Statistics() {
	// TODO Auto-generated destructor stub
}

LoggerPtr Statistics::logger(Logger::getLogger("TriAD"));


void Statistics::updateTotalCardnality() {
	totalCardinality++;
}

bool Statistics::updateCardinality1(unsigned type, unsigned key, unsigned card, bool summary){

	FastMap *map = (summary)? sc_map[type] : c_map[type];
	map->insert(key,card,~0u);
	return true;
}

bool Statistics::updateCardinality1(unsigned type, unsigned key1,unsigned key2, unsigned card, bool summary){
	FastMap *map = (summary)? spc_map[type] : pc_map[type];
	map->insert(key1,key2,card);
	return true;
}

void Statistics::optimize(){
	for(unsigned type = 0; type < 3; type++){
		c_map[type]->optimize(1);
		pc_map[type]->optimize(2);
	}
	sc_map[0]->optimize(1);
	sc_map[1]->optimize(1);
	spc_map[0]->optimize(2);
	spc_map[1]->optimize(2);

	sc_map[1]->print();
	//spc_map[0]->print();
}

unsigned Statistics::getCardinality(unsigned type, unsigned key, bool sumCard) {

	//FastMap *map = (summary || sumCard)? sc_map[type] : c_map[type];
	FastMap *map = (summary || sumCard)? sc_map[type] : cache_sc;
	std::pair<FastMap::iterator,FastMap::iterator> it = map->find(key);
	unsigned card = 0;
	while(it.first != it.second){
		card += it.first.value(2);
		it.first++;
	}
	//std::cout << "card sc: " << card << endl;
	return card;
}

unsigned Statistics::getCardinality(unsigned type, unsigned key1,
		unsigned key2, bool sumCard) {

	//FastMap *map = (summary || sumCard)? spc_map[type] : pc_map[type];
	FastMap *map = (summary || sumCard)? spc_map[type] : cache_pc;
	if(summary)
		key2 = (key2 >> pbits);

	std::pair<FastMap::iterator,FastMap::iterator> it = map->find(key1,key2);

	unsigned card = 0;
	while(it.first != it.second){
		card += it.first.value(3);
		it.first++;
	}
	//std::cout << "card pc: " << card << endl;
	return card;
}

void Statistics::updateAllJoinSelectivity(double all_path_jsel,
		double all_star1_jsel, double all_star2_jsel, bool summary) {

	if(summary){
		this->s_all_path_jsel = all_path_jsel;
		this->s_all_star1_jsel = all_star1_jsel;
		this->s_all_star2_jsel = all_star2_jsel;
	}else{
		this->all_path_jsel = all_path_jsel;
		this->all_star1_jsel = all_star1_jsel;
		this->all_star2_jsel = all_star2_jsel;
	}
}

double Statistics::joinSelectivity(unsigned pattern, bool p1c, unsigned p1,
		bool p2c, unsigned p2) {

	SINGLE_VECTOR_MAP *sel_map;

	if (!p1c && !p2c)
		return all_path_jsel;
	else if (!p1c || !p2c) {
		 if (!p1c && p2c){
			 unsigned temp = p1;  p1 = p2; p2 = temp;
		 }

		 if (join_selectivity_map.find(p1) != join_selectivity_map.end()) {
			sel_map = &join_selectivity_map.at(p1);
			unsigned card = 0;
			for (SINGLE_VECTOR_MAP::iterator it = sel_map->begin();
					it != sel_map->end(); it++) {
				card += it->second[pattern];
			}
			return (double) card / (getCardinality(1,p1,false) * totalCardinality);
		}
	} else {
		if (join_selectivity_map.find(p1) != join_selectivity_map.end()) {
			sel_map = &join_selectivity_map.at(p1);
			if (sel_map->find(p2) != sel_map->end()){
				uint16_t x = 0;//(summary)? 3 : 0;
				double js = (double)sel_map->at(p2)[x+pattern] /getCardinality(1,p1,false);
				js = js/getCardinality(1,p2,false);
				return js;
			}
		}
	}
	return 0.0;
}

double Statistics::getJoinSelectivity(bool s1c, unsigned s1, bool p1c,
		unsigned p1, bool o1c, unsigned o1, bool s2c, unsigned s2, bool p2c,
		unsigned p2, bool o2c, unsigned o2) {

	// Case Path1: Join on Subject-Object (O2 == S1)
	if (!o1c && !s2c)
		if (o1 == s2)
			return joinSelectivity(0, p1c, p1, p2c, p2);


	// Case Path2: Join on Object-Subject (O1 == S2)
	if (!o2c && !s1c)
		if (o2 == s1)
			return joinSelectivity(0, p2c, p2, p1c, p1);


	// Case star1 : Join on Subjects (S1 == S2)
	if (!s1c && !s2c)
		if (s1 == s2)
			return joinSelectivity(1, p1c, p1, p2c, p2);

	// Case star2 : Join on Objects (O1 == O2)
	if (!o1c && !o2c)
		if (o1 == o2)
			return joinSelectivity(2, p1c, p1, p2c, p2);
}

bool Statistics::updateJoinSelectivity(unsigned key1,
		unsigned key2, unsigned path_card,
		unsigned star1_card, unsigned star2_card, bool summary) {

	SINGLE_VECTOR_MAP *sel_map;
	vector<unsigned> selectivity;
	selectivity.push_back(path_card);
	selectivity.push_back(star1_card), selectivity.push_back(star2_card);

	std::pair<SINGLE_VECTOR_MAP::iterator, bool> ret;

	if (join_selectivity_map.find(key1) != join_selectivity_map.end()) {
		sel_map = &join_selectivity_map.at(key1);
		if (sel_map->find(key2) == sel_map->end()) {
			ret =
					sel_map->insert(
							std::pair<unsigned,
									vector<unsigned> >(key2,
									selectivity));
			return ret.second;
		}else if(summary){
			vector<unsigned> *sel = & sel_map->at(key2);
			sel->push_back(path_card); sel->push_back(star1_card); sel->push_back(star2_card);
		}else{
            vector<unsigned> *vect = &sel_map->at(key2);
            unsigned *p = &vect->at(0); *p += path_card;
            unsigned *s1 = &vect->at(1); *s1 += star1_card;
            unsigned *s2 = &vect->at(2); *s2 += star2_card;
		}
		return true;

	} else {
		SINGLE_VECTOR_MAP new_sel_map;
		sel_map = &new_sel_map;
		sel_map->clear();
		ret = sel_map->insert(
				std::pair<unsigned, vector<unsigned> >(
						key2, selectivity));
		if (ret.second) {
			std::pair<JOIN_SEL_MAP::iterator, bool> ret2;
			ret2 = join_selectivity_map.insert(
					std::pair<unsigned, SINGLE_VECTOR_MAP>(key1,
							*sel_map));
			return ret2.second;
		}
		return false;
	}
}

unsigned Statistics::getCardinality(unsigned type, unsigned pred){
	std::pair<FastMap::iterator,FastMap::iterator> it = spc_map[type]->find(pred);

	return (it.second - it.first);
}

void Statistics::getCardinality(unsigned val1, unsigned val1C, unsigned val2,
		unsigned val2C, unsigned val3, unsigned val3C, unsigned &cardinality,
		unsigned &org_cardinality, unsigned order, vector<unsigned> & vect) {
		// make the request into vect array

	Utilities::value32_t card = 0;
	unsigned type = 0;


	if (~val1C) {
		if (~val2C) {
			if (~val3C) {
				cardinality = org_cardinality = 1;
				return; // All constants cardinality = 1;
			} else {
				vect.push_back(1);		// req. pair cardinality
				vect.push_back(order);
				vect.push_back(val1C);
				vect.push_back(val2C);
				return;
			}
		} else {
			vect.push_back(0);		// req. single cardinality
			vect.push_back(order);
			vect.push_back(val1C);
			return ;
		}
	}
	return;
}

void Statistics::getCardinality(unsigned val1,unsigned val1C,
		unsigned val2,unsigned val2C,unsigned val3,unsigned val3C, unsigned &cardinality, unsigned &org_cardinality, unsigned order)
		// Compute the cardinality of a single pattern
		{

	Utilities::value32_t card = 0;
	unsigned type = 0;

	if(summary){
		switch(order){
			case 2: type = 0; break; // PS order
			case 3: type = 1; break; // PO order
		}
	}else{
		switch(order){
			case 0: type = 0; break;
			case 1: type = (!~val2)? 2 : 0; break;
			case 2: type = (!~val2)? 0 : 1; break;
			case 3:	type = 1; break;
			case 4: type = (!~val2)? 1 : 2; break;
			case 5: type = 2; break;
		}
	}
	if (~val1C) {
		if (~val2C) {
			if (~val3C) {
				cardinality = org_cardinality = 1;
				return; // All constants cardinality = 1;
			} else {

				{// perform swaps
						if ((order == 1 && type == 2)
								|| (order == 2 && type == 0)
								|| (order == 4 && type == 1)) {
							std::swap(val1, val2);
							std::swap(val1C, val2C);
						}
				}
				if ((card = getCardinality(type,val1C, val2C, false))){
					if(summary || !pbits){
						cardinality = org_cardinality = card;
						return;
					}
					org_cardinality = card;
					double factor = (double)plist.at(val3) / getCardinality(type,val1C, val2C >> pbits,true);
					cardinality = (double)card * factor;
					if(cardinality < 1)
						cardinality = 1;
					return;
				}
				cardinality = org_cardinality = (summary)? totalSumCardinality : totalCardinality;
				return;
			}
		} else {
			if(summary) type = 1;
			if ((card = getCardinality(type,val1C,false))){
					cardinality = org_cardinality = card;
					return;
			}
			cardinality = org_cardinality = (summary)? totalSumCardinality : totalCardinality;
			//std::cout << order << " " << val1C << " " << cardinality << endl;
			return ;
		}
	} else{
		cardinality = org_cardinality = (summary)? totalSumCardinality : totalCardinality;
		return;
	}


}


void Statistics::dynamicStats(vector<unsigned> &stats_req, vector<unsigned> * stats, unsigned cSize){
	unsigned order, val1, val2, card;
	pair<FastMap::iterator, FastMap::iterator> iter;
	unsigned counter = 0;
	for(vector<unsigned>::iterator it = stats_req.begin(); it != stats_req.end(); it++){
		if(*it == 0){ // type: single cardinality
			order = *(++it);
			val1 = *(++it);
			card = 0;
			for(int i = 1; i < cSize; i++)
				card += stats[i].at(counter);
			cache_sc->insert(val1,card,~0u);
			counter++;

		}
		if(*it == 1){ // type: pair cardinality
			order = *(++it);
			val1 = *(++it);
			val2 = *(++it);
			card = 0;
			for(int i = 1; i < cSize; i++)
				card += stats[i].at(counter);
			cache_pc->insert(val1,val2,card);
			counter++;
		}
	}
	cache_sc->optimize(0);
	cache_pc->optimize(0);

	//cache_sc->print();
}
