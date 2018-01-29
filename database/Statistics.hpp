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

#ifndef STATISTICS_HPP_
#define STATISTICS_HPP_

#include <utils/FastMap.hpp>
#include <utils/Utilities.hpp>

using namespace std;
using namespace log4cxx;

typedef boost::unordered_map<unsigned, vector<unsigned>,
		boost::hash<unsigned> > SINGLE_VECTOR_MAP;

typedef boost::unordered_map<unsigned, SINGLE_VECTOR_MAP,
		boost::hash<unsigned> > JOIN_SEL_MAP;

class Statistics {
private:

	JOIN_SEL_MAP join_selectivity_map;

	FastMap *c_map[3], *pc_map[3], *js_map;

	FastMap *sc_map[3], *spc_map[2];

	double all_path_jsel, all_star1_jsel, all_star2_jsel;
	double s_all_path_jsel, s_all_star1_jsel, s_all_star2_jsel;
	bool summary;

	FastMap *cache_sc, *cache_pc;

public:
	static log4cxx::LoggerPtr logger;
	unsigned pbits;
	vector<unsigned> plist;
	unsigned totalCardinality, totalSumCardinality;

	Statistics();
	virtual ~Statistics();

	bool getEnv(){return summary;};

	bool senEnv(bool env){summary = env;};

	void set_pbits(unsigned pbits){this->pbits = pbits;}

	bool updateCardinality1(unsigned type, unsigned key, unsigned card, bool summary);

	bool updateCardinality1(unsigned type, unsigned key1,
				unsigned key2, unsigned card, bool summary);


	unsigned getCardinality(unsigned type, unsigned key, bool sumCard);

	unsigned getCardinality(unsigned type, unsigned key1,
			unsigned key2, bool sumCard);

	unsigned getCardinality(unsigned type, unsigned pred);

	unsigned getCardnality() {return totalCardinality;}

	unsigned getCardinality(unsigned subjectConstant,unsigned subject,
			unsigned predicateConstant,unsigned predicate, unsigned objectConstant, unsigned object);

	void getCardinality(unsigned subjectConstant, unsigned subject,
			unsigned predicateConstant, unsigned predicate,
			unsigned objectConstant, unsigned object, unsigned &card,
			unsigned &org_card, unsigned order);

	void getCardinality(unsigned subjectConstant, unsigned subject,
			unsigned predicateConstant, unsigned predicate,
			unsigned objectConstant, unsigned object, unsigned &card,
			unsigned &org_card, unsigned order, vector<unsigned> & vect);

	void updateAllJoinSelectivity(double all_path_jsel, double all_star1_jsel,
			double all_star2_jsel, bool summary);

	bool updateJoinSelectivity(unsigned key1,
			unsigned key2, unsigned path_card,
			unsigned star1_card, unsigned star2_card,bool summary);

	double joinSelectivity(unsigned pattern, bool p1c, unsigned p1, bool p2c,
			unsigned p2);

	double getJoinSelectivity(bool s1c, unsigned s1, bool p1c, unsigned p1,
			bool o1c, unsigned o1, bool s2c, unsigned s2, bool p2c, unsigned p2,
			bool o2c, unsigned o2);

	void updateTotalCardnality();

	void optimize();

	void dynamicStats(vector<unsigned> &stats_req, vector<unsigned> * stats, unsigned cSize);

};

#endif /* STATISTICS_HPP_ */

