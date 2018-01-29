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

#include "FastMap.hpp"

using namespace std;

FastMap::FastMap() {
	// TODO Auto-generated constructor stub
	tIndex = new std::vector<TRIPLE>();
}

FastMap::~FastMap() {
	// TODO Auto-generated destructor stub
}

void FastMap::insert(unsigned val1, unsigned val2, unsigned val3){
	TRIPLE t;
	t.val1 = val1; t.val2 = val2; t.val3 = val3;
	tIndex->push_back(t);
}

/*
FastMap::iterator FastMap::find(iterator start, iterator end, unsigned key1, unsigned key2, unsigned key3, unsigned type){
	TRIPLE search_triple;
	search_triple.val1 = key1;
	search_triple.val2 = key2;
	search_triple.val3 = key3;

	if(tIndex->empty())
		return 0;

	unsigned dist = it - iterator(&tIndex->at(0));

	std::vector<TRIPLE>::iterator itv = std::lower_bound(tIndex->begin() + dist, tIndex->end(), search_triple, compare(0));

	TRIPLE *tpstart = &tIndex->at(0) + (itv - tIndex->begin());

	return iterator(tpstart);
}
*/

std::pair<FastMap::iterator,FastMap::iterator> FastMap::find(unsigned key1, unsigned key2, unsigned key3){

	if(!pbits)
		return findS(key1,key2,key3);

	if(tIndex->empty())
		return std::pair<FastMap::iterator,iterator>(0,0);

	TRIPLE search_triple;
	search_triple.val1 = key1;
	search_triple.val2 = key2;
	search_triple.val3 = key3;

	TRIPLE *tpstart, *tpend;

	if((key1 & ~(~0 << pbits)) < 100){
		tpstart = &tIndex->at(0);
		tpend = &tIndex->at(0)+tIndex->size();
	}else{
		std::pair<std::vector<TRIPLE>::iterator, std::vector<TRIPLE>::iterator> it_pair;
		if(key3 < 100 || (key2 & ~(~0 << pbits)) < 100){
			it_pair = std::equal_range(tIndex->begin(), tIndex->end(), search_triple, compare(2,pbits));
		}
		else
			it_pair = std::equal_range(tIndex->begin(), tIndex->end(), search_triple, compare(0,pbits));

		tpstart = &tIndex->at(0) + (it_pair.first -tIndex->begin());
		tpend = &tIndex->at(0) + (it_pair.second -tIndex->begin());
	}

	return std::pair<iterator, iterator>(iterator(tpstart), iterator(tpend));
}

FastMap::iterator FastMap::find(unsigned sdist, unsigned edist, unsigned key1, unsigned key2, unsigned key3){

	if(!pbits)
		return findS(sdist,edist,key1,key2,key3);

	if(tIndex->empty())
		return iterator(0);

	TRIPLE search_triple;
	search_triple.val1 = key1;
	search_triple.val2 = key2;
	search_triple.val3 = key3;

	TRIPLE *tp;

	if((key1 & ~(~0 << pbits)) < 100){
		tp = &tIndex->at(0);
	}else{
		std::vector<TRIPLE>::iterator it;
		if(!~key3 || (key2 & ~(~0 << pbits)) < 100)
			it = std::lower_bound(tIndex->begin() + sdist, tIndex->begin() + edist, search_triple, compare(2,pbits));
		else{
			it = std::lower_bound(tIndex->begin() + sdist, tIndex->begin() + edist, search_triple, compare(0,pbits));
		}
		tp = &tIndex->at(0) + (it - tIndex->begin());
	}

	return iterator(tp);
}

FastMap::iterator FastMap::findS(unsigned sdist, unsigned edist, unsigned key1, unsigned key2, unsigned key3){


	if(tIndex->empty())
		return iterator(0);

	TRIPLE search_triple;
	search_triple.val1 = key1;
	search_triple.val2 = key2;
	search_triple.val3 = key3;

	TRIPLE *tp;

	if(key1 < 100){
		tp = &tIndex->at(0);
	}else{
		std::vector<TRIPLE>::iterator it;
		if(key2 < 100)
			it = std::lower_bound(tIndex->begin() + sdist, tIndex->begin() + edist, search_triple, compare3(1));
		else if(!~key3 || key3 < 100){
			it = std::lower_bound(tIndex->begin() + sdist, tIndex->begin() + edist, search_triple, compare3(2));
		}
		else
			it = std::lower_bound(tIndex->begin() + sdist, tIndex->begin() + edist, search_triple, compare3(0));

		tp = &tIndex->at(0) + (it - tIndex->begin());
	}

	return iterator(tp);
}


std::pair<FastMap::iterator,FastMap::iterator> FastMap::find(unsigned key){
	TRIPLE search_triple;
	search_triple.val1 = key;

	if(tIndex->empty())
		return std::pair<FastMap::iterator,iterator>(0,0);
	std::pair<std::vector<TRIPLE>::iterator, std::vector<TRIPLE>::iterator> it_pair = std::equal_range(tIndex->begin(), tIndex->end(), search_triple, compare3(1));

	TRIPLE *tpstart = &tIndex->at(0) + (it_pair.first -tIndex->begin());
	TRIPLE *tpend = &tIndex->at(0) + (it_pair.second -tIndex->begin());

	return std::pair<iterator, iterator>(iterator(tpstart), iterator(tpend));
}


std::pair<FastMap::iterator,FastMap::iterator> FastMap::find(unsigned key1, unsigned key2){
	TRIPLE search_triple;
	search_triple.val1 = key1;
	search_triple.val2 = key2;

	if(tIndex->empty())
		return std::pair<FastMap::iterator,iterator>(0,0);
	std::pair<std::vector<TRIPLE>::iterator, std::vector<TRIPLE>::iterator> it_pair = std::equal_range(tIndex->begin(), tIndex->end(), search_triple, compare3(2));

	TRIPLE *tpstart = &tIndex->at(0) + (it_pair.first -tIndex->begin());
	TRIPLE *tpend = &tIndex->at(0) + (it_pair.second -tIndex->begin());

	return std::pair<iterator, iterator>(iterator(tpstart), iterator(tpend));
}

std::pair<FastMap::iterator,FastMap::iterator> FastMap::findS(unsigned key1, unsigned key2, unsigned key3){

	if(tIndex->empty())
		return std::pair<FastMap::iterator,iterator>(0,0);

	TRIPLE search_triple;
	search_triple.val1 = key1;
	search_triple.val2 = key2;
	search_triple.val3 = key3;

	TRIPLE *tpstart, *tpend;

	if(key1 < 100){
		tpstart = &tIndex->at(0);
		tpend = &tIndex->at(0)+tIndex->size();
	}else{
		std::pair<std::vector<TRIPLE>::iterator, std::vector<TRIPLE>::iterator> it_pair;
		if(key2 < 100)
			it_pair = std::equal_range(tIndex->begin(), tIndex->end(), search_triple, compare3(1));
		else if(key3 < 100)
			it_pair = std::equal_range(tIndex->begin(), tIndex->end(), search_triple, compare3(2));
		else
			it_pair = std::equal_range(tIndex->begin(), tIndex->end(), search_triple, compare3(0));

		tpstart = &tIndex->at(0) + (it_pair.first -tIndex->begin());
		tpend = &tIndex->at(0) + (it_pair.second -tIndex->begin());
	}

	return std::pair<iterator, iterator>(iterator(tpstart), iterator(tpend));
}



void FastMap::optimize(unsigned type){
	std::sort(tIndex->begin(),tIndex->end(),compare3(type));
	std::vector<TRIPLE>::iterator it = std::unique(tIndex->begin(),tIndex->end(),compare2());
	tIndex->resize(it-tIndex->begin());
}

void FastMap::print(unsigned val1, unsigned val2){
	int count = 0;
	for(vector<TRIPLE>::iterator it = tIndex->begin(); it != tIndex->end(); it++){
		if(it->val1 == val1 && (it->val2 == val2 || val2 < 100)){
			std::cout << it->val1 << " " << it->val2 << " "<< (it->val2 >> pbits) << " " << it->val3 << " "<< (it->val3 >> pbits) << endl;
			count++;
		}
	}
	std::cout << "count:" << count << endl;
}

void FastMap::print(){
        int count = 0;
        for(vector<TRIPLE>::iterator it = tIndex->begin(); it != tIndex->end(); it++){
              std::cout << it->val1 << " " << it->val2 << " " << it->val3 << endl;
              count++;
        }
        std::cout << "count:" << count << endl;
}

void FastMap::clear(){
	delete tIndex;

}

/*
int main() {

	FastMap fm;

	fm.insert(1,2,3);
	fm.insert(1,3,2);
	fm.insert(2,1,3);
	fm.insert(2,3,1);
	fm.insert(3,1,2);
	fm.insert(3,2,1);

	std::pair<FastMap::iterator,unsigned> it_pair;
	it_pair = fm.find(2,3);

	int i = 0;

	while(i++ < it_pair.second){
		std::cout << it_pair.first->val1 << it_pair.first->val2 << it_pair.first->val3 << std::endl;
		it_pair.first++;
	}
}
*/




