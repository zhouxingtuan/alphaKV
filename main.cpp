//
//  main.cpp
//  test
//
//  Created by AppleTree on 17/2/26.
//  Copyright © 2017年 AppleTree. All rights reserved.
//

#include <iostream>
#include <chrono>
#include <time.h>
#include "keyvalue.hpp"
USING_NS_HIVE;

inline int64 get_time_us(void){
	std::chrono::time_point<std::chrono::system_clock> p = std::chrono::system_clock::now();
	return (int64)std::chrono::duration_cast<std::chrono::microseconds>(p.time_since_epoch()).count();
}
inline int64 get_time_ms(void){
	std::chrono::time_point<std::chrono::system_clock> p = std::chrono::system_clock::now();
	return (int64)std::chrono::duration_cast<std::chrono::milliseconds>(p.time_since_epoch()).count();
}

class AlphaKV
{
public:
	typedef KeyValue<65536> KeyValueData;
	KeyValueData* m_pDB;
	CharVector m_buffer;
public:
	AlphaKV(void) : m_pDB(NULL){}
	virtual ~AlphaKV(void){
		closeDB();
	}
	
	bool openDB(const char* name){
		if(NULL != m_pDB){
			return false;
		}
		m_pDB = new KeyValueData(name);
		return (FILE_OK == m_pDB->openDB());
	}
	void closeDB(){
		if(NULL != m_pDB){
			m_pDB->closeDB();
			delete m_pDB;
			m_pDB = NULL;
		}
	}
	char* get(const char* key, uint32 keyLength, uint32* length){
		m_buffer.clear();
		int result = m_pDB->get(key, keyLength, m_buffer);
		if(FILE_OK == result){
			*length = *(int*)(m_buffer.data());
			return m_buffer.data() + sizeof(int);
		}
		return NULL;
	}
	bool set(const char* key, uint32 keyLength, const char* value, uint32 valueLength){
		int result = m_pDB->set(key, keyLength, value, valueLength, true, false);
		return (FILE_OK == result);
	}
};

int main(int argc, const char * argv[]) {
	// insert code here...
	std::cout << "Hello, World!\n";
	
	AlphaKV* pKey = new AlphaKV();
	int64 openT = get_time_us();
	bool result = pKey->openDB("keyvalue");
	int64 openE = get_time_us();
	fprintf(stderr, "open key cost time us = %lld\n", openE-openT);
	if(true == result){
		CharVector value;
		int64 s = get_time_us();
		fprintf(stderr, "s=%lld\n", s);
		int64 getFailedCount = 0;
		int64 setFailedCount = 0;
		char key[32] = {0};
		for(int64 i=1000000;i>=0;--i){
			sprintf(key, "%08lld", i);
			result = pKey->set(key, 8, (char*)&i, sizeof(int64));
			if(false == result){
				++setFailedCount;
			}
			uint32 length;
			char* ptr = pKey->get(key, 8, &length);
			if(NULL == ptr){
				++getFailedCount;
			}
		}
		int64 e = get_time_us();
		fprintf(stderr, "Key e=%lld getFailedCount=%lld setFailedCount=%lld\n", e, getFailedCount, setFailedCount);
		fprintf(stderr, "Key cost us = %lld\n", e-s);
		
		std::string mykey = "welcome";
		std::string myvalue = "hello world!";
		result = pKey->set(mykey.c_str(), (uint32)mykey.length(), myvalue.c_str(), (uint32)myvalue.length());
		fprintf(stderr, "set result = %d\n", (int)result);
		uint32 myvaluelen;
		char* ptr = pKey->get(mykey.c_str(), (uint32)mykey.length(), &myvaluelen);
		if(NULL == ptr){
			fprintf(stderr, "get mykey failed = %s\n", mykey.c_str());
		}else{
			fprintf(stderr, "get myvalue length=%d data=%s\n", myvaluelen, ptr);
		}
		
	}
	delete pKey;
	
    return 0;
}
