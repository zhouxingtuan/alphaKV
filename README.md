# alphaKV
A simple kv db, fast and lightweight. Support Linux/Mac/IOS/Android

# How to Use
#include "alphakv.hpp"
USING_NS_HIVE;
AlphaKV* pKey = new AlphaKV();
bool result = pKey->openDB("alphakv");
result = pKey->set(key, keyLength, value, valueLength);
uint32 valueLength;
char* ptr = pKey->get(key, keyLength, &valueLength);

