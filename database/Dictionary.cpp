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

#include "Dictionary.hpp"

Dictionary::Dictionary() {
	// TODO Auto-generated constructor stub
	nextId = 100;
	dict = new DICTIONARY();
	revDict = new REVERSE_DICT();
	skipStrings = false;

}

Dictionary::Dictionary(unsigned startId) {
	// TODO Auto-generated constructor stub
	nextId = startId;
	dict = new DICTIONARY();
	revDict = new REVERSE_DICT();
	skipStrings = false;
}



Dictionary::~Dictionary() {
	// TODO Auto-generated destructor stub
}

LoggerPtr Dictionary::logger(Logger::getLogger("TriAD"));

bool Dictionary::insert(string key, Utilities::value32_t &id, unsigned type, ostream& out) {

  if (!lookup(key, id)) {
    id = nextId;
    out << key << endl;
    Utilities::hash64_t keyHash = Utilities::hash64(key,0);
    dict->insert(std::make_pair<Utilities::hash64_t, Utilities::value32_t>(keyHash, id));
    nextId++;
  }
  return true;
}

void Dictionary::insert(string text, unsigned id){
	Utilities::hash64_t keyHash = Utilities::hash64(text,0);
	dict->insert(std::make_pair<Utilities::hash64_t, Utilities::value32_t>(keyHash, id));
	if(!skipStrings)
		revDict->insert(std::pair<unsigned,std::string>(id,text));
}

bool Dictionary::insert(string text, Utilities::value32_t &id, Utilities::value32_t &pid, Utilities::value32_t &pid2, unsigned pbits) {

	if(lookup(text,id)) {pid = ~0u;
		if(!skipStrings)
			revDict->insert(std::pair<unsigned,std::string>(id,text));
		return true;
	}

	if(lookup(text,id,0)){pid = id >> pbits;
		if(!skipStrings)
			revDict->insert(std::pair<unsigned,std::string>(id,text));
		return true;
	}

	if(!pbits || !~pid2){
		Utilities::hash64_t keyHash = Utilities::hash64(text,0);
		id = nextId;
		pid = ~0u;
		dict->insert(std::make_pair<Utilities::hash64_t, Utilities::value32_t>(keyHash, id));
		if(!skipStrings)
			revDict->insert(std::pair<unsigned,std::string>(id,text));
		nextId++;
	}else{
	     Utilities::hash64_t keyHash = Utilities::hash64(text,0);
      	     unsigned part;
 	     if(partMap.find(pid2) == partMap.end())
        	part = partMap[pid2] = 100;
	     else{
        	partMap[pid2] = partMap[pid2] + 1;
	        part = partMap[pid2];
     	     }		
	     id = pid2 << pbits | part;	     	
	     dict->insert(std::make_pair<Utilities::hash64_t, Utilities::value32_t>(keyHash, id));
	}

	return true;
}

bool Dictionary::lookup(const std::string &txt, Utilities::value32_t &id) {

	string text = txt;
	Utilities::hash64_t keyHash = Utilities::hash64(text,0);
	if (dict->find(keyHash) != dict->end()) {
		id = dict->at(keyHash);
		return true;
	}
	return false;
}

string Dictionary::lookupText(unsigned id){
	if(revDict->find(id) != revDict->end())
		return revDict->at(id);
	return "";
}

void Dictionary::optimize(bool flag){
	LOG4CXX_INFO(logger, "Optimizing dictionary...");
	for (DICTIONARY::iterator it = dict->begin(); it != dict->end(); it++) {
		dict_item di;
		di.hash = it->first;
		di.id = it->second;
		opt_dict.push_back(di); 
	}
	std::sort(opt_dict.begin(),opt_dict.end(),compare());
	/*for(vector<dict_item>::iterator it = opt_dict.begin(); it != opt_dict.end(); it++){
		std::cout << it->hash << ".. "<< it->id << endl;
	}*/
	if(flag)
		delete dict;
}

bool Dictionary::lookup(const std::string &txt, Utilities::value32_t &id, unsigned type){
	string text = txt;
	Utilities::hash64_t keyHash = Utilities::hash64(text,0);
	dict_item di;
	di.hash = keyHash;
	vector<dict_item>::iterator it;

	it = std::lower_bound(opt_dict.begin(),opt_dict.end(),di,compare());

	if(it != opt_dict.end() && it->hash == keyHash){
		id = it->id;
		return true;
	}
	return false;
}

void Dictionary::addPartInfo(string key, unsigned pid, unsigned pbits){
	unsigned id;
	dict_item di;
	Utilities::hash64_t keyHash = Utilities::hash64(key,0);
	 unsigned part;
     if(partMap.find(pid) == partMap.end())
        part = partMap[pid] = 100;
     else{
        partMap[pid] = partMap[pid] + 1;
        part = partMap[pid];
     }
	di.hash = keyHash; di.id = pid << pbits | part;
	opt_dict.push_back(di);
}



/*
void Dictionary::print(){

  for(std::map<Utilities::value32_t,Utilities::value32_t>::iterator it = partMap.begin(); it != partMap.end(); it++){
        std::cout << it->first << " " << it->second << endl;
  }
}
*/
