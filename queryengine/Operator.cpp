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

#include "Operator.hpp"

Operator::Operator() {
	// TODO Auto-generated constructor stub

}

Operator::~Operator() {
	// TODO Auto-generated destructor stub
}

LoggerPtr Operator::logger(Logger::getLogger("TriAD"));


Relation* Operator::indexScan(Database *db, Database::DataOrder order, BitSet* width, unsigned sub, unsigned pred, unsigned obj, bool full, unsigned joinvar){

	Relation *rel;
	unsigned val1, val2, val3, v1, v2, v3, o1, o2, o3;
	unsigned i = 0; unsigned type = 0;
	if(db->getEnv()){
		v1 = (sub < 100)? sub:~0u, v2 = (pred < 100)? pred:~0u, v3 = (obj < 100)? obj:~0u;
	}else{
		v1 = (sub < 100)? i++:~0u, v2 = (pred < 100)? i++:~0u, v3 = (obj < 100)? i++:~0u;
		if(sub > pred && ~v1 && ~v2) std::swap(v1, v2);
		if(sub > obj && ~v1 && ~v3) std::swap(v1, v3);
		if(pred > obj && ~v2 && ~v3) std::swap(v2, v3);
	}

	switch(order){
	case Database::Order_Subject_Predicate_Object :
		val1 = sub; val2 = pred; val3 = obj; type = 0;
		o1 = v1; o2 = v2; o3 = v3;
		break;
	case Database::Order_Subject_Object_Predicate :
		val1 = sub; val2 = obj; val3 = pred; type = 1;
		o1 = v1; o2 = v3; o3 = v2;
		break;
	case Database::Order_Predicate_Subject_Object :
		val1 = pred; val2 = sub; val3 = obj; type = 2;
		o1 = v2; o2 = v1; o3 = v3;
		break;
	case Database::Order_Predicate_Object_Subject :
		val1 = pred; val2 = obj; val3 = sub; type = 3;
		o1 = v2; o2 = v3; o3 = v1;
		break;
	case Database::Order_Object_Predicate_Subject :
		val1 = obj; val2 = pred; val3 = sub; type = 4;
		o1 = v3; o2 = v2; o3 = v1;
		break;
	case Database::Order_Object_Subject_Predicate :
		val1 = obj; val2 = sub; val3 = pred; type =5;
		o1 = v3; o2 = v1; o3 = v2;
		break;
	}

	if(val1 >= 100)
		if(val2 >= 100 || !~val3)
			rel = new Relation(1);
		else
			rel = new Relation(2);
	else
		rel = new Relation(3);

	if(rel){
		rel->winfo = width;
		rel->pbits = db->tripleIndex->pbits;

		if(db->getEnv()){
			rel->joinvar = joinvar;
			rel->sload(db->tripleIndex,type,o1,o2,o3,val1,val2,val3,full);
		}
		else
			rel->load(db->tripleIndex,type,o1,o2,o3,val1,val2,val3,full);
	}
	return rel;
}
bool Operator::getBindings(Relation *srel,vector<Relation*> plan, unsigned seq, unsigned bindings[]){

	vector<Relation*>::iterator it = plan.begin()+seq;

	if(it == plan.end()){
		//std::cout << srel->width << endl;
		for (int i = 0; i < srel->width; i++) {
			if(bindings[i] && ~bindings[i]){
				//std::cout << i << " " << bindings[i] << endl;
				srel->bit_binds[i]->set(bindings[i]-100);
			}
		}
		//std::cout << "0" << endl;
		return false;
	}
	/*for (int i = 0; i < srel->width; i++) {
            std::cout << i << "::" << bindings[i] << "->";
        }
	std::cout << endl;
	*/
	//std::cout << "1" << endl;
	unsigned o1 = (*it)->o1, o2 = (*it)->o2, o3 = (*it)->o3;
	unsigned val1 = ~0u, val2 = ~0u, val3 = ~0u;

	unsigned joinvar = (*it)->joinvar;
	if(it == plan.begin()) joinvar = ~0u;

	if(~o1) val1 = bindings[o1];
	if(~o2) val2 = bindings[o2];
	if(~o3) val3 = bindings[o3];
	//std::cout << "2" << endl;
	bool flag = true;
	/*if (~joinvar) {
		if(!bindings[joinvar] || srel->bit_binds[joinvar]->test(bindings[joinvar]-100))
		//if (srel->bit_binds[joinvar]->test(bindings[joinvar]))
			flag = getBindings(srel, plan, seq + 1, bindings);
		if(!bindings[joinvar])
			return true;
	}*/
	if(~joinvar){
                if(!bindings[joinvar] || srel->bit_binds[joinvar]->test(bindings[joinvar]-100)){
                        getBindings(srel,plan,seq+1,bindings);
                        flag = false;
                }
        }
        //std::cout << seq << " " << o1 << " " << val1 << " " << o2 << " " << val2 << " "<< o3 << " " << val3 << endl;
	//std::cout << "A" << endl;
	if(flag){
		(*it)->find(val1, val2, val3);
		//std::cout << "B" << endl;
		unsigned v1=~0u, v2=~0u, v3=~0u;
		while(!(*it)->finish()){
			if(~o1) v1 = bindings[o1] = (*it)->atS(o1);
			if(~o2) v2 = bindings[o2] = (*it)->atS(o2);
			if(~o3) v3 = bindings[o3] = (*it)->atS(o3);
			//std::cout << "D" << endl;
			getBindings(srel,plan,seq+1,bindings);
			//std::cout << "E" << endl;
			(*it)->nextS();
			//std::cout << "F" << endl;
		}
	//	std::cout << "C" << endl;
		if(~o1) bindings[o1] = val1;
		if(~o2) bindings[o2] = val2;
		if(~o3) bindings[o3] = val3;
	}
	return false;
}

Relation* Operator::getBindings(vector<Relation*> plan, unsigned numBindings, unsigned numPartitions){

	Relation *srel = new Relation(numBindings);
	for(int i =0; i < srel->width; i++)
	srel->bindings[i].clear();

	/*{//DEBUG
		for(vector<Relation*>::iterator iter = plan.begin(); iter != plan.end(); iter++){
			(*iter)->tupleIterator();
			std::cout << "----------" << endl;
		}
	}*/

	unsigned bindings[30] = {};
	rg::Timer timer;
	timer.start();
	srel->initBitSet(numPartitions);

	getBindings(srel,plan,0,bindings);
	timer.stop();
	LOG4CXX_DEBUG(logger, "Bindings in " << timer);
	return srel;
}

// Merge Join
Relation* Operator::mergeJoin(Relation* left, Relation* right,
		BitSet* left_w, BitSet* right_w, BitSet* width) {

	Relation *newRel = new Relation(width->lengthOf());
	newRel->pbits = left->pbits;
	newRel->full = true;

	if(!left->getSize() || !right->getSize()){
		delete left; delete right;
		return newRel;
	}


	int maxWidth = std::max(log(left_w->valueOf()) / log(2),log(right_w->valueOf()) / log(2)) + 1;

	int left_join_pos = -1, right_join_pos = -1, i = 0;

	for(; i < maxWidth; i++){
		if(left_w->test(i))
			left_join_pos++;
		if(right_w->test(i))
			right_join_pos++;
		if(left_w->test(i) && right_w->test(i))
			break;
	}
	if(i > maxWidth)
		return 0;

	//Perform merge
	int count = 0, count1 = 0, leftC = 0, rightC = 0;
	unsigned lval, rval, joinval;
	while (left->hasNext() && right->hasNext()) {
		lval = left->at(left_join_pos);
		rval = right->at(right_join_pos);
		if (lval < rval){
			left->next(rval,left_join_pos);
		}
		else if (lval > rval){
			right->next(lval,right_join_pos);
		}
		else {
			right->checkpoint();
			joinval = lval;
			while (left->hasNext()) {
				lval = left->at(left_join_pos);
				rval = right->at(right_join_pos);

				if(lval != joinval)	break;

				while (right->hasNext()) {
					rval = right->at(right_join_pos);
					if(rval != joinval) break;
					bool flag = true; int lc = 0, rc = 0;
					for(int i = 0 ; i < maxWidth; i++){

						if(width->test(i) && left_w->test(i) && right_w->test(i) && (left->at(lc) != right->at(rc))){
									flag = false; break;
						}
						if(left_w->test(i))lc++;
						if(right_w->test(i))rc++;
					}

					if(flag){
						int lc = 0, rc = 0, mc = 0, c = 0;
						newRel->numTuples++;
						for(int i = 0; i < maxWidth; i++){
							if(width->test(i)){
								if(left_w->test(i) && right_w->test(i)){
									newRel->loadTuple(left->at(lc),mc);
									lc++; rc++;mc++;
								}
								else {
									if(left_w->test(i)){
										newRel->loadTuple(left->at(lc), mc);mc++; lc++;
									}
									if(right_w->test(i)){
										newRel->loadTuple(right->at(rc), mc);mc++; rc++;
									}
								}
							}
						}
					}
					right->next();
				}
				right->restore();
				left->next();
			}
		}
	}
	delete left;
	delete right;
	return newRel;
}


// Hash Join
Relation* Operator::hashJoin(Relation* left, Relation* right, BitSet *left_w, BitSet *right_w, BitSet* width ) {

	typedef boost::unordered_map<Utilities::value32_t, vector<Utilities::value32_t>,
				boost::hash<Utilities::value32_t> > HASHMAP;

	Relation* temp;
	BitSet* temp_bs;

	HASHMAP hashMap;
	uint64_t leftSize = left->getSize();
	uint64_t rightSize = left->getSize();
	uint64_t m = 0, n = 0;

	if (leftSize > rightSize) {
		temp = left;
		temp_bs = left_w;
		left = right;
		left_w = right_w;
		right = temp;
		right_w = temp_bs;
	}
	leftSize = left->getSize();
	rightSize = right->getSize();

	Relation* newRel = new Relation(width->lengthOf());
	newRel->pbits = left->pbits;
	newRel->full = true;

	if(!left->getSize() || !right->getSize()){
		delete left; delete right;
		return newRel;
	}
	int maxWidth = std::max(log(left_w->valueOf()) / log(2),log(right_w->valueOf()) / log(2)) + 1;

	int left_join_pos = -1, right_join_pos = -1, i = 0;

	for(;i< maxWidth; i++){
		if(left_w->test(i) && right_w->test(i)){
			left_join_pos++;right_join_pos++;
			break;
		}else{
			if(left_w->test(i))
				left_join_pos++;
			if(right_w->test(i))
				right_join_pos++;
		}
	}
	if(i == maxWidth)
		return 0;

	unsigned val = 0;
	// Load left relation into a map
	while(left->hasNext()){
		val = left->at(left_join_pos);
		if (hashMap.find(val) == hashMap.end()){
			vector<Utilities::value32_t> vect;
			vect.push_back(m);
			hashMap.insert(std::pair<Utilities::value32_t, vector<Utilities::value32_t> >(val, vect));
			//filter.insert(val);
		}
		else{
			vector<Utilities::value32_t> *vect;
			vect = &hashMap.at(val);
			vect->push_back(m);
		}
		left->next();m++;
	}
	left->reset();
	rg::Timer t; t.start(); t.stop();
	unsigned count = 0, count1 = 0, count2 = 0, count3 = 0;
	while(right->hasNext()) {
		uint32_t joinVal = right->at(right_join_pos);
		if (hashMap.find(joinVal) != hashMap.end()) {
			vector<Utilities::value32_t> *leftTuples = &hashMap.at(joinVal);
			for (vector<Utilities::value32_t>::iterator it = leftTuples->begin();
					it != leftTuples->end(); it++) {
					bool flag = true; int lc = 0, rc = 0;
					for(int i = 0; i < maxWidth; i++){
					if(width->test(i) && left_w->test(i) && right_w->test(i) && (left->at(lc,*it) != right->at(rc))){
								flag = false;  break;

					}
					if(left_w->test(i)) lc++;
					if(right_w->test(i)) rc++;
				}
				if(flag){
					int lc = 0, rc = 0, mc =0;
					newRel->numTuples++;
					for(int i = 0; i < maxWidth; i++){
						if(width->test(i)){
							if(left_w->test(i) && right_w->test(i)){
								newRel->loadTuple(right->at(rc),mc);
								lc++; rc++; mc++;
							}
							else {
								if(left_w->test(i)){
									newRel->loadTuple(left->at(lc,*it), mc); mc++; lc++;
								}
								if(right_w->test(i)){
									newRel->loadTuple(right->at(rc), mc); mc++; rc++;
								}
							}
						}
					}
				}
			}
		}
		right->next();
	}
	delete left;
	delete right;
	return newRel;
}
