# alphaKV
A simple key-value database, fast and lightweight. Support Linux/Mac/IOS/Android

# How to Use

    #include "alphakv.hpp"
    USING_NS_HIVE;
    AlphaKV* pKey = new AlphaKV();
    bool result = pKey->openDB("mydb");
    result = pKey->set(key, keyLength, value, valueLength);
    uint32 valueLength;
    char* value = pKey->get(key, keyLength, &valueLength);

# More
1) You can redefine the ALPHAKV_HASH_SLOT to get a suitable hash size, which is define in alphakv.hpp file

    #define ALPHAKV_HASH_SLOT 65536

2) You can change the minimum size of storage cost for every value saving, which is define in file.hpp file

    #define BLOCK_SIZE 64

If you want to know more, read the source code 233



