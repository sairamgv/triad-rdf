/*
 * Trie.hpp
 *
 *  Created on: Apr 14, 2015
 *      Author: sairam
 */

#ifndef UTILS_TRIE_HPP_
#define UTILS_TRIE_HPP_

using namespace std;
class Trie {
	typedef struct node{
		string prefix;
		map<unsigned,struct node> *child;
	} NODE;

private:
	NODE *root;

public:
	Trie();
	virtual ~Trie();

	void init();

	NODE* get_new_node();

	void* add(string data);

	string getString(void* ptr);

};

#endif /* UTILS_TRIE_HPP_ */
