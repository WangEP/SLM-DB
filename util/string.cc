/*****************************************************************
 * File: String.cpp
 * Author: Keith Schwarz (htiek@cs.stanford.edu)
 *
 * Implementation of the String type defined in String.hh.
 */

#include "leveldb/string.h"

/* Implementation-specific includes. */
#include <algorithm>
#include <cctype>
#include <cstring>
#include <iterator>

/* Default constructor sets up a small string with no elements. */
String::String() {
  isSmallString = true;
  impl.smallString.size = 0;
}

/* Character constructor sets up a small string with one element. */
String::String(char ch) {
  isSmallString = true;
  impl.smallString.size = 1;
  impl.smallString.elems[0] = ch;
}

/* Conversion constructor tries to use a small string if possible,
 * but falls back on a large string if this will not work.
 */
String::String(const char* str) {
  /* Compute the length of the string once, since strlen can be
   * expensive.
   */
  const size_t length = std::strlen(str);

  /* We will be a small string if the number of characters does not
   * exceed the buffer size.
   */
  isSmallString = (length <= kMaxSmallStringSize);

  /* If we aren't a small string, allocate external storage and transfer
   * the C string.
   */
  if (!isSmallString) {
    impl.largeString.logicalLength   = length;
    impl.largeString.allocatedLength = length;
    impl.largeString.elems = new char[impl.largeString.allocatedLength];
  }
  else
    impl.smallString.size = (unsigned char)length;

  strncpy(data(), str, length);
}

/* Conversion constructor tries to use a small string if possible,
 * but falls back on a large string if this will not work.
 */
String::String(const char* str, size_t length) {

  /* We will be a small string if the number of characters does not
   * exceed the buffer size.
   */
  isSmallString = (length <= kMaxSmallStringSize);

  /* If we aren't a small string, allocate external storage and transfer
   * the C string.
   */
  if (!isSmallString) {
    impl.largeString.logicalLength   = length;
    impl.largeString.allocatedLength = length;
    impl.largeString.elems = new char[impl.largeString.allocatedLength];
  }
  else
    impl.smallString.size = (unsigned char)length;

  strncpy(data(), str, length);
}

/* String copy constructor is a bit tricky.  We want to ensure that we copy over
 * the data without having any shared references, but at the same time don't want
 * to lose the small string optimization.
 */
String::String(const String& other) {
  /* Copy over whether we're a small string. */
  isSmallString = other.isSmallString;

  /* If we're a large string, allocate an external buffer for the string. */
  if (!isSmallString) {
    impl.largeString.logicalLength = other.impl.largeString.logicalLength;
    impl.largeString.allocatedLength = other.impl.largeString.allocatedLength;
    impl.largeString.elems = new char[impl.largeString.allocatedLength];
  }
    /* Otherwise, copy over the string size. */
  else
    impl.smallString.size = other.impl.smallString.size;

  strncpy(data(), other.c_str(), other.size());
}

/* This assignment operator is implemented using the copy-and-swap pattern.  If
 * you are not familiar with this pattern, consider reading "Effective C++, 3rd Edition"
 * by Scott Meyers, which has a great explanation of the technique.
 */
String& String::operator= (const String& other) {
  String copy(other);
  swap(copy);
  return *this;
}

/* The String destructor only needs to clean up memory if we are using a large
 * string.
 */
String::~String() {
  if (!isSmallString)
    delete [] impl.largeString.elems;
}

char* String::data() {
  return isSmallString? impl.smallString.elems : impl.largeString.elems;
}

/* In order to convert the string into a C-style string, we need to allocate a buffer
 * and append a null terminator.  We'll use our own buffer for this, which may
 * trigger a reallocation.  However, getting a C-style string is semantically const,
 * and so we will need to use a const_cast to trick the compiler into thinking that
 * we're doing something safe.
 */
const char* String::c_str() const {
  return isSmallString ? impl.smallString.elems : impl.largeString.elems;
}

/* The capacity of the string is:
 * 1. kMaxSmallStringSize if the string is small.
 * 2. Found in the LargeString object otherwise.
 */
size_t String::capacity() const {
  return isSmallString? kMaxSmallStringSize : impl.largeString.allocatedLength;
}

/* The size may be in one of two places depending on the implementation,
 * so we much check how we are implemented first.
 */
size_t String::size() const {
  return isSmallString? impl.smallString.size : impl.largeString.logicalLength;
}

/* We're empty if we have size zero.  Note that implementing empty() in terms of
 * size is generally not a good idea, but since size() runs in O(1) this is both
 * safe and a good idea from an encapsulation perspective.
 */
bool String::empty() const {
  return size() == 0;
}

/* Our implementation of swap simply calls the STL version of this method on each data
 * member.
 */
void String::swap(String& other) {
  std::swap(isSmallString, other.isSmallString);

  /* Notice that we don't need any special logic to swap the implementations
   * based on whether one is a small string, etc.  By swapping all of the bytes
   * in the union, we're guaranteed to copy everything over correctly.
   */
  std::swap(impl, other.impl);
}


/* Fast comparing algorithm that supposed to work faster than strcmp()
 * Ref: http://mgronhol.github.io/fast-strcmp/
 */

int compare(const String& one, const String& two) {
  const int len = (one.size() < two.size()) ? one.size() : two.size();
  const char* ptr0 = one.c_str();
  const char* ptr1 = two.c_str();
  int fast = len/sizeof(size_t) + 1;
  int offset = (fast-1)*sizeof(size_t);
  int current_block = 0;

  if( len <= sizeof(size_t)){ fast = 0; }

  size_t *lptr0 = (size_t*)ptr0;
  size_t *lptr1 = (size_t*)ptr1;

  while( current_block < fast ){
    if( (lptr0[current_block] ^ lptr1[current_block] )){
      int pos;
      for(pos = current_block*sizeof(size_t); pos < len ; ++pos ){
        if( (ptr0[pos] ^ ptr1[pos]) || (ptr0[pos] == 0) || (ptr1[pos] == 0) ){
          return  (int)((unsigned char)ptr0[pos] - (unsigned char)ptr1[pos]);
        }
      }
    }

    ++current_block;
  }

  while( len > offset ){
    if( (ptr0[offset] ^ ptr1[offset] )){
      return (int)((unsigned char)ptr0[offset] - (unsigned char)ptr1[offset]);
    }
    ++offset;
  }
  if (one.size() < two.size()) return -1;
  else if (one.size() > two.size()) return 1;
  return 0;
}

/* The implementation of < is a lexicographical comparison, done compare() function
 */
bool operator < (const String& one, const String& two) {
  return compare(one, two) < 0;
}

/* Equality comparison is done by checking that the sizes agree and that the characters
 * are all equal.
 */
bool operator== (const String& one, const String& two) {
  return one.size() == two.size() && compare(one, two) == 0;
}

/* The rest of the relational operators are implemented in terms of < and ==. */
bool operator <= (const String& one, const String& two)  {
  return !(two < one);
}

bool operator != (const String& one, const String& two) {
  return !(one == two);
}

bool operator >= (const String& one, const String& two) {
  return !(one < two);
}

bool operator > (const String& one, const String& two) {
  return two < one;
}