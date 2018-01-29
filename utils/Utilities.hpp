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

#include <string>
#include <cstdlib>
#include <string>
#include <map>
#include <vector>
#include <list>

#include <boost/unordered_map.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/predicate.hpp>


#include <log4cxx/logger.h>

#ifdef CONFIG_WINDOWS
#include <pstdint.h>
#else
#include <stdint.h>
#endif

#ifndef UTILITIES_H
#define UTILITIES_H

using namespace std;

typedef std::vector<std::string> VECTOR_STRING;
typedef std::vector<uint32_t> VECTOR_INT_32;

// contains static utility functions for hashing and sorting as well as basic data type definitions
class Utilities {

	public:

		// a 32-bit hash key
		typedef unsigned int hash32_t;

		// a 32-bit value
		typedef unsigned value32_t;

		// a 64-bit hash key
		typedef unsigned long long hash64_t; // enforce 64 bits

		// a 64-bit value
		typedef unsigned long long value64_t; // enforce 64 bits

		typedef vector<value32_t> VECTOR_INT_32;

		class StringHash64 {
			public:
				value64_t operator()(const string& s) const {
					return hash64(s, 0);
				}
		};

		/**
		 * Custom equality and hash function for the literals to avoid collisions while inserting into Dictionary.
		 * Add or tweak any desired hash function and equality to see which works best.
		 * The current setting seemed to perform fast enough.
		 */
		template<class T>
		struct CustomConfigDictionary {
				value32_t operator()(const string& key_value) const {
					// here is the computation of the hash function for any KEY_CLASS object
					return hash32(key_value, 0);
				}

				//compare two KEY_CLASS objects
				bool operator()(const string& left, const string& right) const {
					return boost::algorithm::iequals(left, right, std::locale());
				}
		};

		// hash arbitrary data into a 32-bit value
		static hash32_t hash32(const void* buffer, unsigned size,
				hash32_t init = 0);

		// hash a string into a 32-bit value
		static hash32_t hash32(const string& text, hash32_t init = 0);

		// hash arbitrary data into a 64-bit value
		static hash64_t
		hash64(const void* buffer, unsigned size, hash64_t init = 0);

		// hash a string into a 64-bit value
		static hash64_t hash64(const string& text, hash64_t init = 0);



};

#endif

