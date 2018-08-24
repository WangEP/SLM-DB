/**********************************************************
 * File: String.hh
 * Author: Keith Schwarz (htiek@cs.stanford.edu)
 *
 * An implementation of a String class that uses several
 * behind-the-scenes optimizations to improve performance.
 * The main optimization is the small-string optimization.
 * The key idea behind this optimization is that most strings
 * are not particularly long; most are only a handful of
 * characters in length.  Normally, strings are implemented
 * by allocating an external buffer of characters.  In the
 * small string optimization, the implementation instead works
 * by using the object itself as the character buffer for
 * small strings, switching over to an external buffer only
 * when the size exceeds the object's preallocated storage
 * space.  This is implemented in this class using a union of
 * the two implementations.
 */

#ifndef String_Included
#define String_Included

#include <iostream> // For ostream, istream
#include <iterator> // For reverse_iterator

class String {
public:
  /* Constructor: String();
   * Usage: String str;
   * -------------------------------------------------------
   * Constructs a new, empty String.
   */
  String();

  /* Constructor: String(char ch);
   * Usage: String str = 'x';
   * ------------------------------------------------------
   * Constructs a String that is a single character in
   * length.
   */
  explicit String(char ch);

  /* Constructor: String(const char* str);
   * Usage: String str = "This is a string!"
   * -------------------------------------------------------
   * Constructs a new String whose contents are a deep-copy
   * of the specified C-style string.
   */
  explicit String(const char* str);

  /* Constructor: String(const char* str, size_t length);
   * Usage: String str = "This is a string!"
   * -------------------------------------------------------
   * Constructs a new String whose contents are a deep-copy
   * of the specified C-style string.
   */
  String(const char* str, size_t length);


  /* Destructor: ~String();
   * Usage: (implicit)
   * ----------------------------------------------------
   * Deallocates the String and any resources it may have
   * allocated.
   */
  ~String();

  /* Copy constructor: String(const String& other);
   * Usage: String str = otherString;
   * ----------------------------------------------------
   * Creates a new String object that's a full, deep-copy
   * of some other String object.
   */
  String(const String& other);

  /* Assignment operator: String& operator= (const String& other);
   * Usage: str1 = str2;
   * -------------------------------------------------------
   * Sets this String object equal to some other String object.
   */
  String& operator= (const String& other);

  char* data();

  size_t size() const;
  bool   empty() const;

  /* void swap(String& other);
   * Usage: str1.swap(str2);
   * -----------------------------------------------------
   * Exchanges the contents of this String object and some
   * other String object.  This is guaranteed not to throw
   * any exceptions.
   */
  void swap(String& other);

  /* const char* c_str() const;
   * Usage: ifstream input(myStr.c_str());
   * -------------------------------------------------------
   * Returns a C-style string representation of this String
   * object.  This C string is valid until any operation is
   * performed on the String.
   */
  const char* c_str() const;

  /* size_t capacity() const;
   * void reserve(size_t space);
   * Usage: s.reserve(100);
   *        if (s.capacity() >= 100) { ... }
   * -------------------------------------------------------
   * capacity() returns the amount of space allocated in the
   * String's internal storage.  Operations that do not
   * increase the String's size beyond its capacity will
   * proceed more quickly than operations that do.  The
   * reserve(size_t) function ensures that the capacity is
   * at least as large as its argument.  If you know in
   * advance that the String will have a certain size,
   * preemptively reserving space for the String can result
   * in performance increases.
   */
  size_t capacity() const;

private:
  /* A constant dictating how many bytes are used for the small-
   * string optimization.  Higher numbers mean less frequent
   * allocations, but higher memory usage per string.
   */
  static const size_t kMaxSmallStringSize = 31;

  /* A struct encoding information necessary to implement a
   * String using memory external to the string itself.
   */
  struct LargeString {
    char* elems; // Pointer to the elements buffer.
    size_t logicalLength;   // How many characters are used.
    size_t allocatedLength; // How much space is allocated.
  };

  /* A struct encoding information necessary to implement a
   * String using its own space for storage.
   */
  struct SmallString {
    /* We use just one byte to keep track of the storage space. */
    unsigned char size;

    /* The rest of the bytes go to the preallocated buffer. */
    char elems[kMaxSmallStringSize];
  };

  /* A union of the two implementations.  For small strings,
   * smallString is active; for large strings, largeString is
   * active.
   */
  union StringImpl {
    LargeString largeString;
    SmallString smallString;
  };

  /* The implementation of this object, along with a flag controlling
   * which implementation is being used.
   */
  StringImpl impl;
  bool isSmallString;
};

/* Fast string compare */
int compare(const String& lhs, const String& rhs);

/* Relational operators. */
bool operator <  (const String& lhs, const String& rhs);
bool operator <= (const String& lhs, const String& rhs);
bool operator == (const String& lhs, const String& rhs);
bool operator != (const String& lhs, const String& rhs);
bool operator >= (const String& lhs, const String& rhs);
bool operator >  (const String& lhs, const String& rhs);

#endif