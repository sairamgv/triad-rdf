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

#ifndef TRIPLESTORE_HPP_
#define TRIPLESTORE_HPP_

#include <utils/Utilities.hpp>
#include <utils/FastMap.hpp>
#include <set>

using namespace log4cxx;

class TripleStore {

public:
	static log4cxx::LoggerPtr logger;


	vector<unsigned> tmpTripleVector[20];
	unsigned clusterSize;
	set<unsigned> properties;
	Utilities::value64_t numTriples;
	FastMap *dataIndex[6];
	unsigned pbits;

	///Summary Graph Info
	FastMap* summaryIndex[3];
	vector<unsigned> plist;

	TripleStore();
	virtual ~TripleStore();

	void set_pbits(unsigned pbits){this->pbits = pbits;}

	bool addTriple(unsigned subject,  unsigned predicate,
			unsigned object, unsigned cSize);

	void joinCardinality(vector<unsigned> & jCard, bool summary);

	unsigned getLocation(unsigned subjectConstant,
			unsigned predicateConstant, unsigned objectConstant);

	bool add(VECTOR_INT_32 & vect);

	void sort();

	void stats();

	void serializeSummary(vector<unsigned> & vect);

	void computeCardinalities(FastMap*, vector<unsigned> & jCard);

	void computeCardinalities(unsigned type, vector<unsigned> & jCard, bool summary);

	std::pair<FastMap::iterator, FastMap::iterator> getIterator(unsigned type,
				unsigned val1, unsigned val2, unsigned val3);

	void computeQueryStatistics(vector<unsigned> & stats_req, vector<unsigned> & stats);

};

#endif /* TRIPLESTORE_HPP_ */
