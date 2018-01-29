#ifndef H_tools_rdf3xload_TurtleParser
#define H_tools_rdf3xload_TurtleParser

#include <istream>
#include <string>
#include "stdint.h" /* Replace with <stdint.h> if appropriate */

#include "Type.hpp"
#include <utils/Utilities.hpp>

#include <boost/unordered_map.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>

class Parser {

	public:

		Utilities::value32_t indexCounter;
		Utilities::value32_t tupleCounter;
/*

		unsigned subCounter;
		unsigned predCounter;
		unsigned objCounter;
*/

/*
		POSTINGLIST_MAP subjectsMap;
		POSTINGLIST_MAP predicatesMap;
		POSTINGLIST_MAP objectsMap;

*/
		// The map which holds the 32 bit hashed literals vs their indexed value
//		Dictionary dictionary;

		typedef struct triple{
		    Utilities::hash32_t subject;
		    Utilities::hash32_t predicate;
		    Utilities::hash32_t object;
		} HashedTriple;

		HashedTriple hashedTriple;
		// A parse error
		class Exception {
			public:
				// The message
				std::string message;

				// Constructor
				Exception(const std::string& message);
				// Constructor
				Exception(const char* message);
				// Destructor
				~Exception();
		};

	private:

		// A turtle lexer
		class Lexer {
			public:
				// Possible tokens
				enum Token {
					Eof,
					Dot,
					Colon,
					Comma,
					Semicolon,
					LBracket,
					RBracket,
					LParen,
					RParen,
					At,
					Type,
					Integer,
					Decimal,
					Double,
					Name,
					A,
					True,
					False,
					String,
					URI
				};

			private:

				// The input
				std::istream& in;
				// The putback
				Token putBack;
				// The putback string
				std::string putBackValue;
				// Buffer for parsing when ignoring the value
				std::string ignored;
				// The current line
				unsigned line;

				// Size of the read buffer
				static const unsigned readBufferSize = 1024;
				// Read buffer
				char readBuffer[readBufferSize];
				// Read buffer pointers
				char* readBufferStart, *readBufferEnd;

				// Read new characters
				bool doRead(char& c);
				// Read a character
				bool read(char& c) {
					if (readBufferStart < readBufferEnd) {
						c = *(readBufferStart++);
						return true;
					} else
						return doRead(c);
				}
				// Unread the last character
				void unread() {
					readBufferStart--;
				}

				// Lex a hex code
				unsigned lexHexCode(unsigned len);
				// Lex an escape sequence
				void lexEscape(std::string& token);
				// Lex a long string
				Token lexLongString(std::string& token);
				// Lex a string
				Token lexString(std::string& token, char c);
				// Lex a URI
				Token lexURI(std::string& token, char c);
				// Lex a number
				Token lexNumber(std::string& token, char c);

			public:

				// Constructor
				Lexer(std::istream& in);
				// Destructor
				~Lexer();

				// The next token (including value)
				Token next(std::string& value);
				// The next token (ignoring the value)
				Token next() {
					return next(ignored);
				}
				// Put a token and a string back
				void unget(Token t, const std::string& s) {
					putBack = t;
					if (t >= Integer) putBackValue = s;
				}
				// Put a token back
				void ungetIgnored(Token t) {
					putBack = t;
					if (t >= Integer) putBackValue = ignored;
				}
				// Get the line
				unsigned getLine() const {
					return line;
				}
		};
		// A triple
		struct Triple {
				// The entries
				std::string subject, predicate, object, objectSubType;
				// Type for the object
				Type::ID objectType;

				// Constructor
				Triple(const std::string& subject,
						const std::string& predicate,
						const std::string& object, Type::ID objectType,
						const std::string& objectSubType) :
					subject(subject), predicate(predicate), object(object),
							objectSubType(objectSubType),
							objectType(objectType) {
				}
		};

		// The lexer
		Lexer lexer;
		// The uri base
		std::string base;
		// All known prefixes
		std::map<std::string, std::string> prefixes;
		// The currently available triples
		std::vector<Triple> triples;
		// Reader in the triples
		unsigned triplesReader;
		// The next blank node id
		unsigned nextBlank;

		// Is a (generalized) name token?
		static inline bool isName(Lexer::Token token);
		// Convert a relative URI into an absolute one
		void constructAbsoluteURI(std::string& uri);
		// Construct a new blank node
		void newBlankNode(std::string& node);
		// Report an error
		void parseError(const std::string& message);
		// Parse a qualified name
		void parseQualifiedName(const std::string& prefix, std::string& name);
		// Parse a blank entry
		void parseBlank(std::string& entry);
		// Parse a subject
		void parseSubject(Lexer::Token token, std::string& subject);
		// Parse an object
		void parseObject(std::string& object, Type::ID& objectType,
				std::string& objectSubType);
		// Parse a predicate object list
		void parsePredicateObjectList(const std::string& subject,
				std::string& predicate, std::string& object,
				Type::ID& objectType, std::string& objectSubType);
		// Parse a directive
		void parseDirective();
		// Parse a new triple
		void parseTriple(Lexer::Token token, std::string& subject,
				std::string& predicate, std::string& object,
				Type::ID& objectType, std::string& objectSubType);
		// Insert a triple into the dictionary
		void createDictionaryEntry(std::string& subject,
				std::string& predicate, std::string& object);

	public:

		// Constructor
		Parser(std::istream& in);

		// Destructor
		~Parser();

		// Read the next triple
		bool parse(std::string& subject, std::string& predicate,
				std::string& object, Type::ID& objectType,
				std::string& objectSubType);

};
#endif
