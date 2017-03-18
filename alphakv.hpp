//
//  alphakv.h
//  test
//
//  Created by AppleTree on 17/3/18.
//  Copyright © 2017年 AppleTree. All rights reserved.
//

#ifndef alphakv_hpp
#define alphakv_hpp

#include "keyvalue.hpp"

NS_HIVE_BEGIN

#define ALPHAKV_HASH_SLOT 4096

class AlphaKV
{
public:
	typedef KeyValue<ALPHAKV_HASH_SLOT> KeyValueData;
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

NS_HIVE_END

#endif /* alphakv_h */
