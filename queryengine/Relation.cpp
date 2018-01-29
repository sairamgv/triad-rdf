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

#include "Relation.hpp"

Relation::Relation() {
  // TODO Auto-generated constructor stub
  width =  0;
  numTuples = 0;
  id = 0;
  pbits=0;
}

Relation::~Relation() {
  // TODO Auto-generated destructor stub
}

Relation::Relation(uint16_t numAttrs) {
  width =  numAttrs;
  numTuples = 0;
  id = 0;tIt = 0;
  tuples.clear();
}

void Relation::initBitSet(unsigned numPartitions){
	for(int i = 0; i < 30; i++)
		//bit_binds[i] = new bitset<205000>(0);
		bit_binds[i] = new bset(numPartitions+101);
}

void Relation::partitionList(vector<Utilities::value32_t> & plist, vector<Utilities::value32_t> & plist_stats){
	unsigned pos = 0;
	plist.clear();
	plist_stats.clear();
        unsigned bitsize = bit_binds[0]->size();
	for(int i = 0 ; i < 10; i++){
		unsigned count = 0;
		if(bit_binds[i]->any()){
			plist.push_back(i);
			plist.push_back(bit_binds[i]->count());
			plist_stats.push_back(bit_binds[i]->count());
			//TODO: Make the bit_binds size dynamic
			/*for(int j = 0; j < 205000; j++){
				if(bit_binds[i]->test(j)){
				 plist.push_back(j+100);
				 count++;
				}
			}*/
                        int bit_it = bit_binds[i]->find_first();
			while(bit_it < bitsize){
                                //if(bit_binds[i]->test(bit_it)){
                                        plist.push_back(bit_it+100);
                                        count++;
                                //}
                                bit_it = bit_binds[i]->find_next(bit_it);

                        }
			std::cout << i << " " << count << endl; // DEBUG
		}
	}
	return;
}


void Relation::sload(TripleStore *t, uint16_t type, unsigned o1, unsigned o2, unsigned o3, unsigned val1, unsigned val2, unsigned val3, bool full){



	this->val1 = (val1 < 100)? 0 : val1; this->val2 = (val2 < 100)? 0 : val2 ; this->val3 = (val3 < 100)? 0 : val3;
	this->o1 = o1; this->o2 = o2; this->o3 = o3;
	this->full = full;

	if(type != 2 && type != 3){
		std::cout << "Hola" << endl;
		return;
	}
	if(this->val2)
		this->val2 = (!~this->val2)? this->val2 : (this->val2 >> t->pbits);
	if(this->val3)
		this->val2 = (!~this->val2)? this->val2 : (this->val2 >> t->pbits);

	//std::cout << type << " " << o1 << " " << o2 << " " << o3 << " " << this->val1 << " " << this->val2 << " " << this->val3 << endl;


	this->idx = t->summaryIndex[type-2];
	this->it = idx->findS(this->val1,this->val2,this->val3);

	this->start = this->it.first;
	LOG4CXX_INFO(mpi2::detail::logger, "Index scan (size): " << (it.second - it.first) << " " << this->val1);
}

bool Relation::find(unsigned  val1, unsigned  val2, unsigned  val3){

	if(!~val1) val1 = this->val1;
	if(!~val2) val2 = this->val2;
	if(!~val3) val3 = this->val3;

	val1 = (~val1)? val1 : 0; val2 = (~val2)? val2 : 0; val3 = (~val3)? val3 : 0;

	it = idx->findS(val1,val2,val3);
	return false;
}
bool Relation::finish(){
	if(it.first == it.second) return true;
	return false;
}

void Relation::nextS(){
	if(!~val3){
		std::pair<FastMap::iterator, FastMap::iterator> kt = idx->find(val1,it.first->val2);
		it.first = kt.second;
	}else
		it.first++;
}

void Relation::nextS(unsigned val){
	if(width == 1){
		if(!~val3)
			it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val1,val,0);
		else
			it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val1,val2,val);
	}
	else if(width == 2){
		it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val1,val,0);
	}
	else if(width == 3){
		it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val,0,0);
	}
}
void Relation::add(unsigned i, unsigned val){
	bindings[i].insert(bindings[i].end(),val);
}

unsigned Relation::atS(unsigned col){
		if (o1 == col)
			return it.first.value(1);
		if (o2 == col)
			return it.first.value(2);
		if (o3 == col)
			return it.first.value(3);
}
//----------------------------------------------
void Relation::load(TripleStore *t, uint16_t type, unsigned o1, unsigned o2, unsigned o3, unsigned val1, unsigned val2, unsigned val3, bool full){

	//std::cout << type << " " << o1 << " " << o2 << " " << o3 << " " << val1 << " " << val2 << " " << val3 << endl;

	this->o1 = o1; this->o2 = o2; this->o3 = o3;
	this->full = full;
	this->t = t;
	this->val1 = val1; this->val2 = val2; this->val3 = val3;

	if(!t->pbits)
		t->plist.push_back(0); // Insert a dummy partition for non-summary mode



	pstart = t->plist.begin();
	pend = t->plist.end();
	unsigned val = (val2 < 100)? val2 : val3;

	if (t->pbits) {
		while (*pstart != val) {
			if (pstart == pend)
				return;
			pstart += (*(pstart + 1) + 2);
		}
		pit = pstart =  pstart + 2;
		pend = pstart + *(pstart-1);
	}else
		pit = pstart;

	if(pit == pend){
		return;
	}

	this->idx = t->dataIndex[type];
	unsigned v1 = val1, v2 = val2, v3 = val3;

	while (pit != pend) {
		if (this->val1 < 100) v1 = *pit << pbits;
		else if (this->val2 < 100)	v2 = *pit << pbits;
		else if (this->val3 < 100) v3 = *pit << pbits;
		this->it = idx->find(v1, v2, v3);
		if (it.second != it.first)
			break;
		pit++;
	}
	if(pit == pend) return;
	this->start = this->it.first;

	if(!full) return;
	unsigned c = 0;
	while (pit != pend){
		if (this->pbits) {
			val1 = this->val1; val2 = this->val2; val3 = this->val3;
			if (val1 < 100)  val1 = *pit << pbits;
			else if (val2 < 100) val2 = *pit << pbits;
			else if (val3 < 100) val3 = *pit << pbits;
			this->it = idx->find(val1, val2, val3);
		}

		while ((it.first != it.second)) {
			unsigned v1 = it.first->val1;
			unsigned v2 = it.first->val2;
			unsigned v3 = it.first->val3;
			unsigned to1 = o1, to2 = o2, to3 = o3;
			if (to1 > to2) {std::swap(v1, v2);	std::swap(to1, to2);}
			if (to1 > to3) {std::swap(v1, v3);	std::swap(to1, to3);}
			if (to2 > to3) {std::swap(v2, v3);	std::swap(to2, to3);}
			loadTuple(v1, to1);
			loadTuple(v2, to2);
			loadTuple(v3, to3);


			if (!~val3) {
				std::pair<FastMap::iterator, FastMap::iterator> kt = idx->find(it.first->val1,it.first->val2);
				it.first = kt.second;

			} else
				it.first++;
			numTuples++;
		}
		if (this->pbits)
			pit++;
		else
			break;
	}
	return;
}

void Relation::partitionTuples(unsigned jpos, unsigned cSize, unsigned rank, unsigned toLoc){
	unsigned part;
	rg::Timer timer;
	timer.start();
	if(!full){
		while (pit != pend){
			if (this->pbits) {
				unsigned v1 = val1, v2 = val2, v3 = val3;
				if(val1 < 100) v1 = *pit << pbits;
				else if(val2 < 100) v2 = *pit << pbits;
				else if(val3 < 100) v3 = *pit << pbits;
				this->it = idx->find(v1,v2,v3);
			}
			while ((it.first != it.second)) {
					unsigned v1 = it.first->val1;
					unsigned v2 = it.first->val2;
					unsigned v3 = it.first->val3;
					unsigned to1 = o1, to2 = o2, to3 = o3;
					if(to1 > to2) {std::swap(v1,v2);std::swap(to1,to2);}
					if(to1 > to3) {std::swap(v1,v3);std::swap(to1,to3);}
					if(to2 > to3) {std::swap(v2,v3);std::swap(to2,to3);}

					if(~toLoc) part = toLoc;
					else{
						if(jpos == to1) part = (v1>>pbits) % cSize + 1;
						else if(jpos == to2) part = (v2>>pbits) % cSize + 1;
						else if(jpos == to3) part = (v3>>pbits) % cSize + 1;
					}

					loadTuple(v1, to1, part, rank);
					loadTuple(v2, to2, part, rank);
					loadTuple(v3, to3, part, rank);

					if(!~val3){
						unsigned key = it.first->val2;
						std::pair<FastMap::iterator, FastMap::iterator> kt = idx->find(val1,key);
						it.first = kt.second;
					}
					else
						it.first++;
					numTuples++;
			}
			if (this->pbits)
				pit++;
			else
				break;
		}
		full = true;
	}else{
		for(int i = 0; i <= cSize; i++)
			c_tuples[i].clear();

		if(~toLoc){
			c_tuples[toLoc].insert(c_tuples[toLoc].begin(),tuples.begin(), tuples.end());
		}else{
			for (int i = 0; i < numTuples; i++) {
				part = (tuples.at(i * width + jpos)>>pbits) % cSize + 1;
				for (int j = 0; j < width; j++)
					c_tuples[part].push_back(tuples.at(i * width + j));
			}
		}
	}
	tuples.clear();
	tuples.insert(tuples.end(),c_tuples[rank].begin(),c_tuples[rank].end());
	numTuples = tuples.size()/width;
	timer.stop();

}

void Relation::append(vector<unsigned> new_tuples){
	tuples.insert(tuples.end(),new_tuples.begin(),new_tuples.end());
	numTuples += (new_tuples.size()/width);
}


bool Relation::loadTuple(Utilities::value32_t val, unsigned col){
	if(col < 100)
		tuples.push_back(val);
};

bool Relation::loadTuple(Utilities::value32_t val, unsigned col, unsigned part, unsigned rank){
	if(col < 100){
		c_tuples[part].push_back(val);
	}
}


void Relation::tupleIterator(){
	unsigned count = 0;
	if(full){
		for(vector<Utilities::value32_t>::iterator it = tuples.begin(); it != tuples.end(); it++){
			std::cout << *it << " ";
			if(++count%width == 0)
				std::cout << endl;
		}
	}else{
		while(it.first != it.second){
			std::cout << it.first->val1 << " " << it.first->val2 << " " << it.first->val3 << endl;
			it.first++;
		}
		resetS();
	}
}

uint64_t Relation::getSize(){
	if(!full) return it.second - it.first;
	return numTuples;
}

void Relation::reset(){
	if(!full){
		this->it.first = this->start;
	}
	else
		tIt = 0;
}

void Relation::resetS(){
		this->it.first = this->start;
}


void Relation::checkpoint(){
	if(full)
		tmp = tIt;
	else{
		temp = it;
		temp_it = pit;
	}
}

void Relation::restore(){
	if(full)
		tIt = tmp;
	else{
		pit = temp_it;
		it = temp;
	}
}

bool Relation::hasNext(){
	if(!full){
		if(!pbits){
			if(it.second != it.first) return true;
		}
		else{
			if(it.second != it.first && pit != pend) return true;
			unsigned val1 = this->val1, val2 = this->val2, val3 = this->val3;
			while(pit != pend){
				pit++;
				unsigned v1 = val1, v2 = val2, v3 = val3;
				if(val1 < 100) v1 = *pit << pbits;
				else if(val2 < 100) v2 = *pit << pbits;
				else if(val3 < 100) v3 = *pit << pbits;
				this->it = idx->find(v1,v2,v3);

				if(it.first != it.second) return true;

			}
			return false;
		}
	}
	else if(full && tIt < numTuples)
		return true;
	return false;
}


void Relation::next(){
	if(!full){
		if(!~val3){
			std::pair<FastMap::iterator, FastMap::iterator> kt = idx->find(val1,it.first->val2,0);
			it.first = kt.second;
		}else
			it.first++;
		numTuples++;
	}else
		tIt++;
}


void Relation::next(unsigned val){
	if(!full){
		if(width == 1){
			if(!~val3)
				it.first = idx->find(it.first - idx->begin(),it.second - idx->begin(), it.first->val1,val,0);
			else
				it.first = idx->find(it.first - idx->begin(),it.second - idx->begin(), it.first->val1,it.first->val2,val);
		}
		else if(width == 2){
			it.first = idx->find(it.first - idx->begin(),it.second - idx->begin(), it.first->val1,val,0);
		}
		else if(width == 3){
			it.first = idx->find(it.first - idx->begin(),it.second - idx->begin(), val,0,0);
		}
	}else
		tIt++;
}

unsigned Relation::lower_bound(unsigned tIt, unsigned col, unsigned val){
        unsigned start = tIt, end = numTuples - 1, cur;
        unsigned mid;
        while (start < end){
                mid = (start + end)/2;
                cur = tuples.at(mid*width+col);
                if(cur == val)	end = mid;
                else if(cur > val)	end = mid - 1;
                else start = mid + 1;
        }
        if(start+1 < numTuples) if(tuples.at(start*width+col) < val) return start+1;
        return start;
}

void Relation::next(unsigned val, unsigned col){
        if(!full){
                if(width == 1){
                        if(!~val3)
                                it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val1,val,val3);
                        else
                                it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val1,val2,val);
                }
                else if(width == 2){
                        it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val1,val,val3);
                }
                else if(width == 3){
                        it.first = idx->findS(it.first - idx->begin(),it.second - idx->begin(), val,val2,val3);
                }
        }else{
                tIt = lower_bound(tIt+1,col,val);
        }
}


unsigned Relation::at(unsigned col){
	if (!full) {
		unsigned v1 = it.first.value(1);
		unsigned v2 = it.first.value(2);
		unsigned v3 = it.first.value(3);
		unsigned to1 = o1, to2 = o2, to3 = o3;
		if(to1 > to2) {std::swap(v1,v2);std::swap(to1,to2);}
		if(to1 > to3) {std::swap(v1,v3);std::swap(to1,to3);}
		if(to2 > to3) {std::swap(v2,v3);std::swap(to2,to3);}
		if (to1 == col)
			return v1;
		if (to2 == col)
			return v2;
		if (to3 == col)
			return v3;
	}
	return tuples.at(tIt*width+col);
}


unsigned Relation::at(unsigned col, unsigned row){
	if(!full){
		unsigned v1 = (start+row).value(1);
		unsigned v2 = (start+row).value(2);
		unsigned v3 = (start+row).value(3);
		unsigned to1 = o1, to2 = o2, to3 = o3;
		if(to1 > to2) {std::swap(v1,v2);std::swap(to1,to2);}
		if(to1 > to3) {std::swap(v1,v3);std::swap(to1,to3);}
		if(to2 > to3) {std::swap(v2,v3);std::swap(to2,to3);}
		if (to1 == col)
			return v1;
		if (to2 == col)
			return v2;
		if (to3 == col)
			return v3;
	}
	return tuples.at(row*width+col);
}

