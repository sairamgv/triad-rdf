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

#ifndef BITARRAY_HPP_
#define BITARRAY_HPP_

namespace std {

class BitArray {
private:
	unsigned bits[1000000];

public:
	BitArray();
	virtual ~BitArray();

	void add(unsigned val){
		bits[val/32] = bits[val/32] | (1 << (val%32));};

	bool check(unsigned val){
		return (bits[val/32] & (1 << (val%32)))? true:false;};

};

} /* namespace std */
#endif /* BITARRAY_HPP_ */
