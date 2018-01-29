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

#ifndef RELATION_HPP_
#define RELATION_HPP_

#include<vector>
#include<iostream>
#include<stdint.h>

#include <cts/cts.hpp>
#include <utils/BitArray.hpp>
#include <bitset>
#include <mpi2/logger.h>
#include <boost/dynamic_bitset.hpp>
typedef boost::dynamic_bitset<> bset;


using namespace std;
using namespace log4cxx;
using namespace mpi2;


using namespace std;
class Relation {
  private:
    uint64_t id; // Relation Id
    unsigned val1,val2,val3;
    uint64_t tmp;

    FastMap *idx;
    FastMap::iterator  start;
    std::pair<FastMap::iterator,FastMap::iterator> temp;
    vector<Utilities::value32_t>::iterator temp_it;
    TripleStore *t;

  public:
    unsigned o1, o2, o3;
    vector<Utilities::value32_t> tuples;
    vector<Utilities::value32_t> c_tuples[30];
    BitSet* winfo;
    vector<Utilities::value32_t> bindings[30];
    unsigned pbits;
    BitArray *binds[30];
    //TODO: Make the bit_binds size dynamic (use: boost::dynamic_bitset)
    //bitset<205000> *bit_binds[30];
    bset *bit_binds[30];
    unsigned joinvar;

    uint64_t numTuples; // Number of tuples in the relation
    uint16_t width; // No. of attributes/columns in the new relation
    std::pair<FastMap::iterator,FastMap::iterator> it;
    uint64_t tIt;
    vector<Utilities::value32_t>::iterator pit, pstart,pend;
    bool full;
    unsigned loc;
    unsigned cSize;


    Relation();
    virtual ~Relation();

    Relation(uint16_t width);

    void initBitSet(unsigned numPartitions);

    void load(TripleStore *t, uint16_t type, unsigned o1, unsigned o2, unsigned o3, unsigned val1, unsigned val2, unsigned val3, bool full);

    void sload(TripleStore *t, uint16_t type, unsigned o1, unsigned o2, unsigned o3, unsigned val1, unsigned val2, unsigned val3, bool full);

    uint16_t getWidth(){return width;}

    uint64_t getSize();

    void setWidth(uint16_t numAttrs){width = numAttrs;}

    bool loadTuple(Utilities::value32_t val, unsigned col);

    void tupleIterator(int count);

    void tupleIterator();

    void serialize(vector<Utilities::value32_t> & vect);

    void deSerialize(vector<Utilities::value32_t> & vect);

    void partitionList(vector<Utilities::value32_t> & plist, vector<Utilities::value32_t> & plist_stats);

    void next();

    void next(unsigned val);

    void next(unsigned val, unsigned col);

    unsigned lower_bound(unsigned tIt, unsigned col, unsigned val);

    bool hasNext();

    unsigned inc();

    void checkpoint();

    void restore();

    void reset();

    unsigned at(unsigned col);

    unsigned at(unsigned col, unsigned row);

    void append(vector<unsigned> new_tuples);

    bool loadTuple(Utilities::value32_t val, unsigned col, unsigned part, unsigned rank);

    void partitionTuples(unsigned jpos, unsigned cSize, unsigned rank, unsigned toLoc);
    //-------------------------------------------------
    bool find(unsigned  val1, unsigned val2, unsigned  val3);

    bool finish();

    void nextS();

    bool check(unsigned i, unsigned val);

    void add(unsigned i, unsigned val);

    unsigned atS(unsigned col);

    void nextS(unsigned val);

    void resetS();


};

#endif /* RELATION_HPP_ */
