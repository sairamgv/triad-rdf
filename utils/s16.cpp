/*
Copyright 2014 Martin Theobald

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

//----------------------------------------------------------------------------------------------------------
//	Contains compression and decompression algorithms
//	for the Simple-16 compression scheme, similar to S-9.
//----------------------------------------------------------------------------------------------------------

#include <iostream>
#include <stdlib.h>

#include "s16.hpp"

#include <rg/evaluation.h> // use this for timing

using namespace std;

unsigned int Simple16::s16_encode(unsigned int* buffer, unsigned int* outBuffer, int size) {

  int cInsert = 0, word, shift, tmp, off = 0, num = size, l, j;

  // outer loops: keep on iterating over all S16 cases
  for (int k = 0; k < 16 && off < size; k++) {

    word = (k << 28) & 0xf0000000; // encode current case into upper 4 bits of output word
    num = (S16_CNUM_28[k] < num) ? S16_CNUM_28[k] : num; // amount of values that can be captured by this case
    shift = 0; // initial shift in the data bits is 0

    for (j = off, l = 0; j < size && l < num; j++, l++) {
      tmp = buffer[j];
      if (tmp >= (1 << 28)) {
        cerr << "\n\nINVALID INPUT FOR S16 COMPRESSION: " << tmp << " EXCEEDS 28 BITS!\n" << endl;
        return 0;
      }

      if (tmp < (1 << S16_CBITS_28[k][l])) { // check if current input integer fits into bits range of current case k and data value l
        word += tmp << shift;
        shift += S16_CBITS_28[k][l];
      } else
        break; // does not fit into current width -> give up on this case and try next one
    }

    if (l == num) { // we have a full series of input integers that fit into the current case k
      S16_CASES_USED[k]++; // just for printing the case statistics
      outBuffer[cInsert++] = word;
      off = j;
      num = size - j;
      k = -1;
    }
  }

  return cInsert;
}

unsigned int Simple16::s16_decode(unsigned int* _w, unsigned int* _p) {
  int _k = (*_w) >> 28;
  switch (_k) {
    case 0:
      *_p = (*_w) & 1;
      _p++;
      *_p = (*_w >> 1) & 1;
      _p++;
      *_p = (*_w >> 2) & 1;
      _p++;
      *_p = (*_w >> 3) & 1;
      _p++;
      *_p = (*_w >> 4) & 1;
      _p++;
      *_p = (*_w >> 5) & 1;
      _p++;
      *_p = (*_w >> 6) & 1;
      _p++;
      *_p = (*_w >> 7) & 1;
      _p++;
      *_p = (*_w >> 8) & 1;
      _p++;
      *_p = (*_w >> 9) & 1;
      _p++;
      *_p = (*_w >> 10) & 1;
      _p++;
      *_p = (*_w >> 11) & 1;
      _p++;
      *_p = (*_w >> 12) & 1;
      _p++;
      *_p = (*_w >> 13) & 1;
      _p++;
      *_p = (*_w >> 14) & 1;
      _p++;
      *_p = (*_w >> 15) & 1;
      _p++;
      *_p = (*_w >> 16) & 1;
      _p++;
      *_p = (*_w >> 17) & 1;
      _p++;
      *_p = (*_w >> 18) & 1;
      _p++;
      *_p = (*_w >> 19) & 1;
      _p++;
      *_p = (*_w >> 20) & 1;
      _p++;
      *_p = (*_w >> 21) & 1;
      _p++;
      *_p = (*_w >> 22) & 1;
      _p++;
      *_p = (*_w >> 23) & 1;
      _p++;
      *_p = (*_w >> 24) & 1;
      _p++;
      *_p = (*_w >> 25) & 1;
      _p++;
      *_p = (*_w >> 26) & 1;
      _p++;
      *_p = (*_w >> 27) & 1;
      _p++;
      break;
    case 1:
      *_p = (*_w) & 3;
      _p++;
      *_p = (*_w >> 2) & 3;
      _p++;
      *_p = (*_w >> 4) & 3;
      _p++;
      *_p = (*_w >> 6) & 3;
      _p++;
      *_p = (*_w >> 8) & 3;
      _p++;
      *_p = (*_w >> 10) & 3;
      _p++;
      *_p = (*_w >> 12) & 3;
      _p++;
      *_p = (*_w >> 14) & 1;
      _p++;
      *_p = (*_w >> 15) & 1;
      _p++;
      *_p = (*_w >> 16) & 1;
      _p++;
      *_p = (*_w >> 17) & 1;
      _p++;
      *_p = (*_w >> 18) & 1;
      _p++;
      *_p = (*_w >> 19) & 1;
      _p++;
      *_p = (*_w >> 20) & 1;
      _p++;
      *_p = (*_w >> 21) & 1;
      _p++;
      *_p = (*_w >> 22) & 1;
      _p++;
      *_p = (*_w >> 23) & 1;
      _p++;
      *_p = (*_w >> 24) & 1;
      _p++;
      *_p = (*_w >> 25) & 1;
      _p++;
      *_p = (*_w >> 26) & 1;
      _p++;
      *_p = (*_w >> 27) & 1;
      _p++;
      break;
    case 2:
      *_p = (*_w) & 1;
      _p++;
      *_p = (*_w >> 1) & 1;
      _p++;
      *_p = (*_w >> 2) & 1;
      _p++;
      *_p = (*_w >> 3) & 1;
      _p++;
      *_p = (*_w >> 4) & 1;
      _p++;
      *_p = (*_w >> 5) & 1;
      _p++;
      *_p = (*_w >> 6) & 1;
      _p++;
      *_p = (*_w >> 7) & 3;
      _p++;
      *_p = (*_w >> 9) & 3;
      _p++;
      *_p = (*_w >> 11) & 3;
      _p++;
      *_p = (*_w >> 13) & 3;
      _p++;
      *_p = (*_w >> 15) & 3;
      _p++;
      *_p = (*_w >> 17) & 3;
      _p++;
      *_p = (*_w >> 19) & 3;
      _p++;
      *_p = (*_w >> 21) & 1;
      _p++;
      *_p = (*_w >> 22) & 1;
      _p++;
      *_p = (*_w >> 23) & 1;
      _p++;
      *_p = (*_w >> 24) & 1;
      _p++;
      *_p = (*_w >> 25) & 1;
      _p++;
      *_p = (*_w >> 26) & 1;
      _p++;
      *_p = (*_w >> 27) & 1;
      _p++;
      break;
    case 3:
      *_p = (*_w) & 1;
      _p++;
      *_p = (*_w >> 1) & 1;
      _p++;
      *_p = (*_w >> 2) & 1;
      _p++;
      *_p = (*_w >> 3) & 1;
      _p++;
      *_p = (*_w >> 4) & 1;
      _p++;
      *_p = (*_w >> 5) & 1;
      _p++;
      *_p = (*_w >> 6) & 1;
      _p++;
      *_p = (*_w >> 7) & 1;
      _p++;
      *_p = (*_w >> 8) & 1;
      _p++;
      *_p = (*_w >> 9) & 1;
      _p++;
      *_p = (*_w >> 10) & 1;
      _p++;
      *_p = (*_w >> 11) & 1;
      _p++;
      *_p = (*_w >> 12) & 1;
      _p++;
      *_p = (*_w >> 13) & 1;
      _p++;
      *_p = (*_w >> 14) & 3;
      _p++;
      *_p = (*_w >> 16) & 3;
      _p++;
      *_p = (*_w >> 18) & 3;
      _p++;
      *_p = (*_w >> 20) & 3;
      _p++;
      *_p = (*_w >> 22) & 3;
      _p++;
      *_p = (*_w >> 24) & 3;
      _p++;
      *_p = (*_w >> 26) & 3;
      _p++;
      break;
    case 4:
      *_p = (*_w) & 3;
      _p++;
      *_p = (*_w >> 2) & 3;
      _p++;
      *_p = (*_w >> 4) & 3;
      _p++;
      *_p = (*_w >> 6) & 3;
      _p++;
      *_p = (*_w >> 8) & 3;
      _p++;
      *_p = (*_w >> 10) & 3;
      _p++;
      *_p = (*_w >> 12) & 3;
      _p++;
      *_p = (*_w >> 14) & 3;
      _p++;
      *_p = (*_w >> 16) & 3;
      _p++;
      *_p = (*_w >> 18) & 3;
      _p++;
      *_p = (*_w >> 20) & 3;
      _p++;
      *_p = (*_w >> 22) & 3;
      _p++;
      *_p = (*_w >> 24) & 3;
      _p++;
      *_p = (*_w >> 26) & 3;
      _p++;
      break;
    case 5:
      *_p = (*_w) & 15;
      _p++;
      *_p = (*_w >> 4) & 7;
      _p++;
      *_p = (*_w >> 7) & 7;
      _p++;
      *_p = (*_w >> 10) & 7;
      _p++;
      *_p = (*_w >> 13) & 7;
      _p++;
      *_p = (*_w >> 16) & 7;
      _p++;
      *_p = (*_w >> 19) & 7;
      _p++;
      *_p = (*_w >> 22) & 7;
      _p++;
      *_p = (*_w >> 25) & 7;
      _p++;
      break;
    case 6:
      *_p = (*_w) & 7;
      _p++;
      *_p = (*_w >> 3) & 15;
      _p++;
      *_p = (*_w >> 7) & 15;
      _p++;
      *_p = (*_w >> 11) & 15;
      _p++;
      *_p = (*_w >> 15) & 15;
      _p++;
      *_p = (*_w >> 19) & 7;
      _p++;
      *_p = (*_w >> 22) & 7;
      _p++;
      *_p = (*_w >> 25) & 7;
      _p++;
      break;
    case 7:
      *_p = (*_w) & 15;
      _p++;
      *_p = (*_w >> 4) & 15;
      _p++;
      *_p = (*_w >> 8) & 15;
      _p++;
      *_p = (*_w >> 12) & 15;
      _p++;
      *_p = (*_w >> 16) & 15;
      _p++;
      *_p = (*_w >> 20) & 15;
      _p++;
      *_p = (*_w >> 24) & 15;
      _p++;
      break;
    case 8:
      *_p = (*_w) & 31;
      _p++;
      *_p = (*_w >> 5) & 31;
      _p++;
      *_p = (*_w >> 10) & 31;
      _p++;
      *_p = (*_w >> 15) & 31;
      _p++;
      *_p = (*_w >> 20) & 15;
      _p++;
      *_p = (*_w >> 24) & 15;
      _p++;
      break;
    case 9:
      *_p = (*_w) & 15;
      _p++;
      *_p = (*_w >> 4) & 15;
      _p++;
      *_p = (*_w >> 8) & 31;
      _p++;
      *_p = (*_w >> 13) & 31;
      _p++;
      *_p = (*_w >> 18) & 31;
      _p++;
      *_p = (*_w >> 23) & 31;
      _p++;
      break;
    case 10:
      *_p = (*_w) & 63;
      _p++;
      *_p = (*_w >> 6) & 63;
      _p++;
      *_p = (*_w >> 12) & 63;
      _p++;
      *_p = (*_w >> 18) & 31;
      _p++;
      *_p = (*_w >> 23) & 31;
      _p++;
      break;
    case 11:
      *_p = (*_w) & 31;
      _p++;
      *_p = (*_w >> 5) & 31;
      _p++;
      *_p = (*_w >> 10) & 63;
      _p++;
      *_p = (*_w >> 16) & 63;
      _p++;
      *_p = (*_w >> 22) & 63;
      _p++;
      break;
    case 12:
      *_p = (*_w) & 127;
      _p++;
      *_p = (*_w >> 7) & 127;
      _p++;
      *_p = (*_w >> 14) & 127;
      _p++;
      *_p = (*_w >> 21) & 127;
      _p++;
      break;
    case 13:
      *_p = (*_w) & 1023;
      _p++;
      *_p = (*_w >> 10) & 511;
      _p++;
      *_p = (*_w >> 19) & 511;
      _p++;
      break;
    case 14:
      *_p = (*_w) & 16383;
      _p++;
      *_p = (*_w >> 14) & 16383;
      _p++;
      break;
    case 15:
      *_p = (*_w) & ((1 << 28) - 1);
      _p++;
      break;
  }

  return S16_CNUM_28[_k];
}
