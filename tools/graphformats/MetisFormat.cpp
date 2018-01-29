/*
 * MetisFormat.cpp
 *
 *  Created on: Apr 29, 2014
 *      Author: sairam
 */

#include "MetisFormat.hpp"

namespace std {

MetisFormat::MetisFormat() {
	// TODO Auto-generated constructor stub
	dict = new Dictionary(1);
	graph = new FastMap();

}

MetisFormat::~MetisFormat() {
	// TODO Auto-generated destructor stub
}

bool MetisFormat::processTriple(string subject, string predicate, string object, ostream& out_dict, ostream& oidf){

	unsigned subIdx, objIdx;

	bool ret_dict = (dict->insert(subject, subIdx,0, out_dict) && dict->insert(object, objIdx,0, out_dict));
	if (!ret_dict)
		std::cout << "Dict Failed" << endl;
	if(subIdx == objIdx) return true;
	oidf << subIdx << " " << objIdx << endl;
	
	graph->insert(subIdx,objIdx,0);
	graph->insert(objIdx,subIdx,0);
	return true;
}

void MetisFormat::printGraph(string path, unsigned num_v){
	cout << "Preparing METIS input format in " << path << endl;
	ofstream out;
	out.open(path.c_str());

	if (!out.is_open()) {
	     cout << "Unable to open output file: " << path;
	     return;
	}

	unsigned src = ~0u;
	cout << num_v << " " << graph->size()/2 + graph->size()%2 << endl;
	out << num_v << " " << graph->size()/2 + graph->size()%2<< endl;

	/*for (FastMap::iterator it = graph->begin(); it != graph->end(); it++) {
		out << it->val1 << " " << it->val2 << endl;
	}*/

	for (FastMap::iterator it = graph->begin(); it != graph->end(); it++) {
		unsigned src = it->val1;
		while (it != graph->end()) {
			if (it->val1 != src) {
				it = it - 1;
				break;
			}
			out << it->val2 << " ";
			it++;
		}
		if (it == graph->end())
			break;
		out << endl;
	}
	out.close();
}

void MetisFormat::printGraph2(string path, unsigned num_v){
	cout << "Preparing METIS input format in " << path << endl;
	ofstream out;
	out.open(path.c_str());

	if (!out.is_open()) {
	     cout << "Unable to open output file: " << path;
	     return;
	}

	unsigned src = ~0u;
	cout << num_v << " " << (graph->size())/2 + graph->size()%2 << endl;
	out << num_v << " " << (graph->size())/2 + graph->size()%2 << endl;

	for(int i = 1; i <= num_v; i++){
		std::pair<FastMap::iterator, FastMap::iterator> iter = graph->find(i);
		while(iter.first != iter.second){
			out << iter.first->val2 << " ";
			iter.first++;
		}
		if(i < num_v)
		out << endl;
	}

	out.close();
}


void MetisFormat::transform(string in, string out){

	string idf = out;
        string mf = out;
        string idformat = idf.append(".idf");
        string mformat = mf.append(".mf");
	string odict = out.append(".dict");
	unsigned num_v;
	readTriples(in,idformat,odict);
	num_v = dict->nextId - 1;
	//num_v = loadGraphData(in, idformat);
	graph->optimize(0);
	cout << "---------------------------------------------------------------------------" << endl;
	cout << "Graph stats: Entites: " << dict->nextId << endl;
	cout << "Graph stats: Edges: " << graph->size() << endl;
	cout << "---------------------------------------------------------------------------" << endl;



	printGraph2(mformat, num_v);
	//graph->print();

}

// reads edge list
// format: "a b"
// assumption graphs contain incremental ids

unsigned MetisFormat::loadGraphData(string dataFile, string idformat) {

	ifstream dataIn(dataFile.c_str());
	string triple; unsigned count = 0;
	unsigned maxID = 0;

	ofstream out;
	out.open(idformat.c_str());

	map<unsigned,unsigned> remap;
	  unsigned src, target, rsrc, rtarget;
	while (dataIn.good() && getline(dataIn,triple)) {


	  if(triple.substr(0,2) == "# ") continue;

	  istringstream iss(triple);
	  iss >> src >> target;

	  if(src == target) continue;

	  if(remap.find(src) == remap.end()){
		  remap[src] = maxID++;
	  }
	  if(remap.find(target) == remap.end()){
	  	  remap[target] = maxID++;
	  }
	  //src = src+1; target = target+1;

	  out << remap[src] << " " << remap[target] << endl;


	  //if(src > maxID) maxID = src;
	  //if(target > maxID) maxID = target;


	  graph->insert(remap[src],remap[target],0);
 	  graph->insert(remap[target],remap[src],0);

 	  if(count > 100000) break;
 	  if(++count % MAX_TUPLES == 0){
	    std::cout << "triples read (so far)... " << count << endl;
	  }
	}
	out.close();
	return maxID;
}


void MetisFormat::readTriples(string path, string idf, string out_dict) {

	rg::Timer timer;
	timer.start();

	unsigned count = 0, countfile = 0;

	DIR *dir;
	struct dirent *ent;
	ofstream od, oidf, omf;
        
	od.open(out_dict.c_str());
	oidf.open(idf.c_str());
	bool isFile = false;

	string ext = path.substr(path.length()-2);
	if(ext == "n3" || ext == "nt") 
           isFile = true;


	if(isFile || (dir = opendir(path.c_str())) != NULL){
		while (isFile || (ent = readdir (dir)) != NULL) {
			string file = (isFile)? path: (ent->d_name);
			//string file(ent->d_name);
			if(isFile || file.length() > 3){
				string ext = file.substr(file.length()-2);
				if(isFile || ext == "n3" || ext == "nt"){
					string str;
					str.append(path);
					if(!isFile){
						str.append("/"); str.append(file);
					}
					//std::cout << countfile++ << " Reading file " << file << " " << endl;
					//LOG4CXX_DEBUG(logger,  countfile++ << " Reading file " << file << " ");
					countfile++;

					ifstream inputStream(str.c_str());
                                       
					Parser parser(inputStream);
					try {

						string subject, predicate, object, objectSubType;
						Type::ID objectType;

						while (true) {
							//if(count > 200000000) break;
							if (count && count % 1000000 == 0){
								cout << "Triples read (so far): " << count << " " << countfile << endl;

							}

							try {
								if (!parser.parse(subject, predicate, object, objectType,
										objectSubType))
									break;
								if(objectType == Type::Literal)
									continue;
								if (!(processTriple(subject, predicate, object,od,oidf) && ++count)){
									cout << "Error parsing triple.." <<std::cout << subject << " " << predicate << " " << object << endl;
									break;
								}

							} catch (const Parser::Exception& e) {
								cout << "Parse Exception " << e.message << endl;
								// recover...
								while (inputStream.get() != '\n')
									;
								continue;
							}
						}
					} catch (const Parser::Exception& e) {
						cout << "Parse Exception " << e.message << endl;
						return;
					}
				}
			}
			if(isFile)
				break;
		 }
	}
	od.close();
        oidf.close();
	timer.stop();
	cout << "Parsed " << count << " in " << timer << endl;

}



}/* namespace std */


