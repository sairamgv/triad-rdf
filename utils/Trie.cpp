/*
 * Trie.cpp
 *
 *  Created on: Apr 14, 2015
 *      Author: sairam
 */

#include "Trie.hpp"

Trie::Trie() {
	// TODO Auto-generated constructor stub

}

Trie::~Trie() {
	// TODO Auto-generated destructor stub
}

void Trie::init(){
	root = get_new_node();
}

Trie::NODE* Trie::get_new_node(){

	NODE* new_node = new NODE();
	new_node->child = new map<unsigned,NODE>();
	return new_node;
}

void* Trie::add(string str){
	// for each character

	NODE *cur_node = root;

	for(char & c : str){

	}
}
