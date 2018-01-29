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


#include "Utilities.hpp"

using namespace std;

static const unsigned goldenRatioHigh = 0x9e3779b9;
static const unsigned goldenRatioLow = 0x7f4a7c13;

static const Utilities::hash64_t goldenRatio =
		(static_cast<Utilities::hash64_t> (goldenRatioHigh) << 32)
				| goldenRatioLow;

#define mix32(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<<8); \
  c -= a; c -= b; c ^= (b>>13); \
  a -= b; a -= c; a ^= (c>>12);  \
  b -= c; b -= a; b ^= (a<<16); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>3);  \
  b -= c; b -= a; b ^= (a<<10); \
  c -= a; c -= b; c ^= (b>>15); \
}

#define mix64(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>43); \
  b -= c; b -= a; b ^= (a<<9); \
  c -= a; c -= b; c ^= (b>>8); \
  a -= b; a -= c; a ^= (c>>38); \
  b -= c; b -= a; b ^= (a<<23); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>35); \
  b -= c; b -= a; b ^= (a<<49); \
  c -= a; c -= b; c ^= (b>>11); \
  a -= b; a -= c; a ^= (c>>12); \
  b -= c; b -= a; b ^= (a<<18); \
  c -= a; c -= b; c ^= (b>>22); \
}

Utilities::hash32_t Utilities::hash32(const void* buffer, unsigned length,
		hash32_t init) {
	register const unsigned char* key =
			static_cast<const unsigned char*> (buffer);
	register hash32_t a = 0x9E3779B9, b = 0x9E3779B9, c = init;
	// Hash the main part
	while (length >= 12) {
		a += (static_cast<hash32_t> (key[0])) | (static_cast<hash32_t> (key[1])
				<< 8) | (static_cast<hash32_t> (key[2]) << 16)
				| (static_cast<hash32_t> (key[3]) << 24);
		b += (static_cast<hash32_t> (key[4])) | (static_cast<hash32_t> (key[5])
				<< 8) | (static_cast<hash32_t> (key[6]) << 16)
				| (static_cast<hash32_t> (key[7]) << 24);
		c += (static_cast<hash32_t> (key[8])) | (static_cast<hash32_t> (key[9])
				<< 8) | (static_cast<hash32_t> (key[10]) << 16)
				| (static_cast<hash32_t> (key[11]) << 24);
		mix32(a, b, c);
		key += 12;
		length -= 12;
	}
	// Hash the tail (max 11 bytes)
	c += length;
	switch (length) {
		case 11:
			c += static_cast<hash32_t> (key[10]) << 24;
		case 10:
			c += static_cast<hash32_t> (key[9]) << 16;
		case 9:
			c += static_cast<hash32_t> (key[8]) << 8;
		case 8:
			b += static_cast<hash32_t> (key[7]) << 24;
		case 7:
			b += static_cast<hash32_t> (key[6]) << 16;
		case 6:
			b += static_cast<hash32_t> (key[5]) << 8;
		case 5:
			b += static_cast<hash32_t> (key[4]);
		case 4:
			a += static_cast<hash32_t> (key[3]) << 24;
		case 3:
			a += static_cast<hash32_t> (key[2]) << 16;
		case 2:
			a += static_cast<hash32_t> (key[1]) << 8;
		case 1:
			a += static_cast<hash32_t> (key[0]);
		default:
			break;
	}
	mix32(a, b, c);

	return c;
}

Utilities::hash32_t Utilities::hash32(const std::string& text, hash32_t init) {
	return hash32(text.c_str(), text.length(), init);
}

Utilities::hash64_t Utilities::hash64(const void* buffer, unsigned length,
		hash64_t init) {
	register const unsigned char* key =
			static_cast<const unsigned char*> (buffer);
	register hash64_t a = init, b = init, c = goldenRatio;
	// Hash the main part
	while (length >= 24) {
		a += (static_cast<hash64_t> (key[0])) | (static_cast<hash64_t> (key[1])
				<< 8) | (static_cast<hash64_t> (key[2]) << 16)
				| (static_cast<hash64_t> (key[3]) << 24)
				| (static_cast<hash64_t> (key[4]) << 32)
				| (static_cast<hash64_t> (key[5]) << 40)
				| (static_cast<hash64_t> (key[6]) << 48)
				| (static_cast<hash64_t> (key[7]) << 56);
		b += (static_cast<hash64_t> (key[8])) | (static_cast<hash64_t> (key[9])
				<< 8) | (static_cast<hash64_t> (key[10]) << 16)
				| (static_cast<hash64_t> (key[11]) << 24)
				| (static_cast<hash64_t> (key[12]) << 32)
				| (static_cast<hash64_t> (key[13]) << 40)
				| (static_cast<hash64_t> (key[14]) << 48)
				| (static_cast<hash64_t> (key[15]) << 56);
		c += (static_cast<hash64_t> (key[16]))
				| (static_cast<hash64_t> (key[17]) << 8)
				| (static_cast<hash64_t> (key[18]) << 16)
				| (static_cast<hash64_t> (key[19]) << 24)
				| (static_cast<hash64_t> (key[20]) << 32)
				| (static_cast<hash64_t> (key[21]) << 40)
				| (static_cast<hash64_t> (key[22]) << 48)
				| (static_cast<hash64_t> (key[23]) << 56);
		mix64(a, b, c);
		key += 24;
		length -= 24;
	}
	// Hash the tail (max 23 bytes)
	c += length;
	switch (length) {
		case 23:
			c += static_cast<hash64_t> (key[22]) << 56;
		case 22:
			c += static_cast<hash64_t> (key[21]) << 48;
		case 21:
			c += static_cast<hash64_t> (key[20]) << 40;
		case 20:
			c += static_cast<hash64_t> (key[19]) << 32;
		case 19:
			c += static_cast<hash64_t> (key[18]) << 24;
		case 18:
			c += static_cast<hash64_t> (key[17]) << 16;
		case 17:
			c += static_cast<hash64_t> (key[16]) << 8;
		case 16:
			b += static_cast<hash64_t> (key[15]) << 56;
		case 15:
			b += static_cast<hash64_t> (key[14]) << 48;
		case 14:
			b += static_cast<hash64_t> (key[13]) << 40;
		case 13:
			b += static_cast<hash64_t> (key[12]) << 32;
		case 12:
			b += static_cast<hash64_t> (key[11]) << 24;
		case 11:
			b += static_cast<hash64_t> (key[10]) << 16;
		case 10:
			b += static_cast<hash64_t> (key[9]) << 8;
		case 9:
			b += static_cast<hash64_t> (key[8]);
		case 8:
			a += static_cast<hash64_t> (key[7]) << 56;
		case 7:
			a += static_cast<hash64_t> (key[6]) << 48;
		case 6:
			a += static_cast<hash64_t> (key[5]) << 40;
		case 5:
			a += static_cast<hash64_t> (key[4]) << 32;
		case 4:
			a += static_cast<hash64_t> (key[3]) << 24;
		case 3:
			a += static_cast<hash64_t> (key[2]) << 16;
		case 2:
			a += static_cast<hash64_t> (key[1]) << 8;
		case 1:
			a += static_cast<hash64_t> (key[0]);
		default:
			break;
	}
	mix64(a, b, c);

	return c;
}

Utilities::hash64_t Utilities::hash64(const std::string& text, hash64_t init) {
	return hash64(text.c_str(), text.length(), init);
}

