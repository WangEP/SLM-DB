#include <libvmem.h>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <cstring>
#include <cstdint>

constexpr uint64_t POOL_SIZE = 2<<30;
constexpr uint64_t OBJ_SIZE = 1<<27;


void random_str(char* str, uint length) {
  for (uint i = 0; i < length; i++) {
    char ascii_code = rand() % 95 + 33;
    str[i] = ascii_code;
  }
}

int main(int argc, char* argv[]) {
  VMEM* vmp;

  if ((vmp = vmem_create("/mnt/mem/vmem_test", POOL_SIZE)) == NULL) {
    perror("vmem_create");
    exit(1);
  }
  std::vector<char*> objs;
  for (int i = 0; i < POOL_SIZE/OBJ_SIZE; i++) {
    char* ptr = new char[OBJ_SIZE];
    random_str(ptr, OBJ_SIZE);
    objs.push_back(ptr);
  }
  clock_t begin = clock();
  for (const auto& obj : objs) {
    char* ptr;
    if ((ptr = (char*) vmem_malloc(vmp, OBJ_SIZE)) == NULL) {
      perror("vmem_malloc");
      exit(1);
    }
    memcpy(ptr, obj, OBJ_SIZE);
  }
  clock_t end = clock();
  double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
  double bd = static_cast<double>(POOL_SIZE) / elapsed_secs;
  fprintf(stdout, "Bandwidth %fGB/s \n", bd);
}