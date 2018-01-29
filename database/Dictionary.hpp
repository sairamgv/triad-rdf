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

#ifndef DICTIONARY_HPP_
#define DICTIONARY_HPP_

#include <utils/Utilities.hpp>
#include <string>
#include <iostream>
using namespace log4cxx;


class Dictionary {

private:
	static log4cxx::LoggerPtr logger;

	typedef struct id_{unsigned id,pid;} ID;
	typedef std::map<Utilities::hash64_t, unsigned> DICTIONARY;
	typedef std::map<unsigned, std::string> REVERSE_DICT;

	DICTIONARY *dict;
	REVERSE_DICT *revDict;


public:
	unsigned nextId;
	bool skipStrings;

	std::map<unsigned,unsigned> partMap;

	typedef struct dict{
		Utilities::hash64_t hash;
		unsigned id;
	} dict_item;

	std::vector<dict_item> opt_dict,tmp;

	struct compare {
		bool operator()(const dict_item &i, const dict_item &j) {
			return (i.hash < j.hash);
		}
	};

	Dictionary();
	Dictionary(unsigned startId);
	virtual ~Dictionary();

	bool insert(string key, Utilities::value32_t &id, unsigned type, ostream& out);

	bool insert(string key, unsigned &id, unsigned &pid, unsigned &pid2, unsigned pbits);

	// custom dictionaries
	void insert(string text, unsigned id);

	bool lookup(const std::string &txt, unsigned &id);

	bool lookup(const std::string& text, unsigned &id, unsigned eType, bool t);

	bool lookup(const std::string& text, unsigned &id, unsigned eType);

	string lookupText(unsigned id);

	void addPartInfo(string key, unsigned partInfo, unsigned pbits);

	void optimize(bool flag);


};

#endif /* DICTIONARY_HPP_ */
