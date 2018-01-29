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
#include <database/Database.hpp>
#include <util/evaluation.h>
#include <math.h>
#include <boost/unordered_map.hpp>

#ifndef OPERATOR_HPP_
#define OPERATOR_HPP_

using namespace log4cxx;

class Operator {
private:
	static log4cxx::LoggerPtr logger;

	enum Op {
		IndexScan, HashJoin, MergeJoin
	};

public:
	Operator();
	virtual ~Operator();

	static Relation* indexScan(Database* db, Database::DataOrder order,
			BitSet* width, unsigned sub, unsigned pred, unsigned obj,
			bool full, unsigned joinvar);

	static Relation* mergeJoin(Relation *left, Relation *right, BitSet *left_w,
			BitSet *right_w, BitSet *width);

	static Relation* hashJoin(Relation *left, Relation *right, BitSet *left_w,
			BitSet *right_w, BitSet *width);

	static Relation* getBindings(vector<Relation*> plan, unsigned numBindings, unsigned numPartitions);

	static void getBindings(Relation *srel, vector<Relation*> plan);

	static bool getBindings(Relation *srel, vector<Relation*> plan,
			unsigned seq, unsigned bindings[]);

	static void getBindings(Relation *srel, Relation* left, Relation *right,
			BitSet* left_w, BitSet* right_w);

};

#endif
