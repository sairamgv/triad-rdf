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

#ifndef FASTMAP_HPP_
#define FASTMAP_HPP_

#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cassert>

class FastMap {
public:
	typedef struct triple{
			unsigned val1,val2,val3;
	} TRIPLE;

	std::vector<TRIPLE> *tIndex;
	unsigned pbits;

	struct compare3 {
		int p;

		compare3(int p){this->p = p;}
		bool operator()(const TRIPLE &i, const TRIPLE &	j) {
			if(p == 1)
				return i.val1 < j.val1;
			if(p == 2)
				return (i.val1 == j.val1)? i.val2 < j.val2: i.val1 < j.val1;

			return (i.val1 == j.val1)? ((i.val2 == j.val2)? i.val3 < j.val3 : i.val2 < j.val2): i.val1 < j.val1;
		}

	};


	struct compare {
		int p;
		unsigned pbits;
		compare(int p, unsigned pbits){this->p = p; this->pbits = pbits;}
		bool operator()(const TRIPLE &i, const TRIPLE &j) {
			unsigned iv1, iv2, iv3, jv1, jv2, jv3;

			if(pbits && !((i.val1 & ~(~0 << pbits)) && (j.val1 & ~(~0 << pbits)))) {
				iv1 = i.val1 >> pbits; jv1 = j.val1 >> pbits;
			}else{
				iv1 = i.val1; jv1 = j.val1;
			}
			if(pbits && !((i.val2 & ~(~0 << pbits)) && (j.val2 & ~(~0 << pbits)))) {
				iv2 = i.val2 >> pbits; jv2 = j.val2 >> pbits;
			}else{
				iv2 = i.val2; jv2 = j.val2;
			}

			if(pbits && !((i.val3 & ~(~0 << pbits)) && (j.val3 & ~(~0 << pbits)))) {
				iv3 = i.val3 >> pbits; jv3 = j.val3 >> pbits;
			}else{
				iv3 = i.val3; jv3 = j.val3;
			}

			//std::cout << "(" << iv1 << "," << jv1 << ") (" << iv2 << "," << jv2 << ") ("  << iv3 << "," << jv3 << ")" << std::endl;

			if(p == 1)
				return iv1 < jv1;
			if(p == 2)
				return (iv1 == jv1)? iv2 < jv2 : iv1 < jv1;

			return (iv1 == jv1)? ((iv2 == jv2)? iv3 < jv3: iv2 < jv2) : iv1 < jv1;
		}

	};
	struct compare2 {
		bool operator()(const TRIPLE &i, const TRIPLE &j) {
			return (i.val1 == j.val1) && (i.val2 == j.val2) && (i.val3 == j.val3);
		}

	};

	class iterator {
	public:
		typedef iterator self_type;
		typedef TRIPLE value_type;
		typedef TRIPLE& reference;
		typedef TRIPLE* pointer;
		typedef std::forward_iterator_tag iterator_category;
		typedef int difference_type;
		iterator(pointer ptr) :	ptr_(ptr) {}
		iterator():	ptr_(0) {}
		unsigned value(unsigned idx){
			if(idx == 1) return ptr_->val1;
			if(idx == 2) return ptr_->val2;
			if(idx == 3) return ptr_->val3;
		}
		self_type operator++() {self_type i = *this;ptr_++;	return i;}
		self_type operator++(int junk) {ptr_++; return *this;}
		reference operator*() {return *ptr_;}
		pointer operator->() {return ptr_;}
		bool operator==(const self_type& rhs) {return ptr_ == rhs.ptr_;}
		bool operator!=(const self_type& rhs) {return ptr_ != rhs.ptr_;}
		unsigned operator-(const self_type& rhs) {return (ptr_ - rhs.ptr_);}
		self_type operator-(const int i) {return iterator(ptr_-i);}
		self_type operator+(const int i) {return iterator(ptr_+i);}
	private:
		pointer ptr_;
	};


	iterator begin() {
		if(tIndex->empty())
			return 0;
		return iterator(&tIndex->at(0));
	}

	iterator end() {
		if(tIndex->empty())
			return 0;
		return iterator(&tIndex->at(0)+tIndex->size());
	}


	iterator cache;

	FastMap();
	virtual ~FastMap();

	void insert(unsigned val1, unsigned val2, unsigned val3);

	std::pair<iterator,iterator> find(unsigned key);

	iterator find(iterator it, unsigned key);

	std::pair<iterator,iterator> find(unsigned key1, unsigned key2);


	iterator find(unsigned start, unsigned end, unsigned key1, unsigned key2, unsigned key3);

	iterator findS(unsigned start, unsigned end, unsigned key1, unsigned key2, unsigned key3);

	std::pair<FastMap::iterator,FastMap::iterator> find(unsigned key1, unsigned key2, unsigned key3);

	std::pair<FastMap::iterator,FastMap::iterator> findS(unsigned key1, unsigned key2, unsigned key3);

	unsigned size(){return tIndex->size();};

	void optimize(unsigned type); // Performs optimizations for fast search

	void print(unsigned val1, unsigned val2);

	void print();

	void clear();

};


#endif /* FASTMAP_HPP_ */

