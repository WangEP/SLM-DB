#ifndef LEVELDB_FAST_ATOI_H
#define LEVELDB_FAST_ATOI_H


int fast_atoi( const char * str )
{
  int val = 0;
  while( *str && *str >= '0' && *str <= '9') {
    val = val*10 + (*str++ - '0');
  }
  return val;
}

#endif //LEVELDB_FAST_ATOI_H
