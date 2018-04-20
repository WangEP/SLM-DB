g++ -o nvm_btree_test.exe ../port/port_posix.cc nvm_btree_test.cc nvm_btree.cc  -I../include -I../ -std=c++11 -DLEVELDB_PLATFORM_POSIX -lpthread
./nvm_btree_test.exe
rm -rf ./nvm_btree_test.exe
