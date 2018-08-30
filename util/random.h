// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_RANDOM_H_
#define STORAGE_LEVELDB_UTIL_RANDOM_H_

#include <cstdint>
#include <cmath>
#include <random>
#include <cfloat>
#include <climits>
#include "coding.h"

namespace leveldb {

class Random {
 private:
  static constexpr uint64_t ITEM_COUNT = 10000000000L;
  static constexpr double ZETAN = 26.46902820178302;
  static constexpr double ZIPFIAN_CONSTANT = 0.99;


  uint32_t seed_;
  uint64_t items_;
  uint64_t base_;
  double zipfianconstant_;
  double alpha_, zetan_, eta_, theta_, zeta2theta_;
  int64_t countforzeta_;
  int64_t last_number_;
  std::random_device rd;  //Will be used to obtain a seed for the random number engine
  std::mt19937 gen; //Standard mersenne_twister_engine seeded with rd()
  std::uniform_real_distribution<> dis;

public:

  explicit Random(uint32_t s, uint64_t items = 0, double zipfianconstant = 0.0) :
  seed_(s & 0x7fffffffu),
  items_(items),
  zipfianconstant_(zipfianconstant),
  gen(rd()),
  dis(0.0, DBL_MAX) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
    if (zipfianconstant_ == 0.0) {
      zipfianconstant_ = ZIPFIAN_CONSTANT;
    }
    if (items_ == 0) {
      items_ = ITEM_COUNT;
      zetan_ = ZETAN;
    }
    base_ = 0;
    theta_ = zipfianconstant_;
    zeta2theta_ = zeta(2, theta_);
  }

  long Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  long Uniform(long n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  long Skewed(long max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }

  long Zipfian(long itemcount) {
    if (itemcount != countforzeta_) {
      if (itemcount > countforzeta_) {
        zetan_ = zeta(countforzeta_, itemcount, theta_, zetan_);
        eta_ = (1 - std::pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
      } else {
        std::__throw_range_error("itemcount too small");
      }
    }
    double u = dis(gen);
    double uz = u * zetan_;
    if (uz < 1.0) {
      return base_;
    }
    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return base_ + 1;
    }
    uint64_t ret = base_ + (uint64_t) (itemcount * std::pow(eta_ * u - eta_ + 1, alpha_));
    return ret;
  }

  long Zipfian() {
    long ret = Zipfian(items_);
    ret = base_ + fnvhash64(ret) % items_;
    last_number_ = ret;
    return ret;
  }

private:

  double zeta(int64_t n, double theta_val) {
    countforzeta_ = n;
    return zetastatic(n, theta_val);
  }

  double zetastatic(int64_t n, double theta) {
    return zetastatic(0, n, theta, 0);
  }

  double zeta(int64_t st, int64_t n, double theta_val, double initialsum) {
    countforzeta_ = n;
    return zetastatic(st, n, theta_val, initialsum);
  }

  double zetastatic(int64_t st, int64_t n, double theta, double initialsum) {
    double sum = initialsum;
    for (int64_t i = st; i < n; i++) {
      sum += 1 / std::pow(i + 1, theta);
    }
    return sum;
  }

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_RANDOM_H_
