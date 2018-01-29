/*
 * MetisFormat.hpp
 *
 *  Created on: Apr 29, 2014
 *      Author: sairam
 */

#ifndef METISFORMAT_HPP_
#define METISFORMAT_HPP_

#include <database/Database.hpp>
#include <parser/Parser.hpp>
#include <utils/FastMap.hpp>
#include <util/evaluation.h>
#include <iostream>
#include <istream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <dirent.h>



namespace std {

class MetisFormat {
public:

	Dictionary *dict;
	FastMap *graph;

	MetisFormat();
	virtual ~MetisFormat();

	void transform(string in, string out_dict);

	void readTriples(string path, string idf, string out_dict);

	// returns num of vertices!
	unsigned loadGraphData(string dataFile, string idformat);

	void printGraph(string path, unsigned num_v);

	void printGraph2(string path, unsigned num_v);

	bool processTriple(string sub, string pred, string obj,ostream& out_dict, ostream& oidf);


};

} /* namespace std */

#endif /* METISFORMAT_HPP_ */
