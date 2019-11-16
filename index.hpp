//
//  index.hpp
//  base
//
//  Created by AppleTree on 16/12/25.
//  Copyright © 2016年 AppleTree. All rights reserved.
//

#ifndef index_hpp
#define index_hpp

#include "file.hpp"

NS_HIVE_BEGIN

#define INDEX_HEAD_OFFSET 32
#define MAX_INDEX_KEY_LENGTH 16

template <typename _TYPE_>
class Index : public File
{
public:
	typedef struct IndexStorage {
		_TYPE_ value;					// value的数值
		uint64 key;
		inline void setKey(uint64 k){
			key = k;
		}
	} IndexStorage;
	typedef struct KeyValue {
		_TYPE_ value;
		int64 offset;
		KeyValue(_TYPE_ value, int64 offset) : value(value), offset(offset){}
		KeyValue(void) : value(0), offset(0) {}
		KeyValue(const KeyValue& other) : value(other.value), offset(other.offset) {}
		inline KeyValue& operator=(const KeyValue& other){ this->value = other.value; this->offset = other.offset; return *this; }
	}KeyValue;
	typedef std::unordered_map<uint64, KeyValue> KeyValueMap;
	typedef std::vector<_TYPE_> NodeVector;
	typedef std::vector<int64> OffsetVector;

	uint64 m_valueSize;					// 保存value的长度
	uint64 m_keyLength;					// key的长度上限
	uint64 m_unitSize;					// key存储单元的长度
	uint64 m_blockSize;					// data存储单元的长度
	KeyValueMap m_keyMapArray;
	OffsetVector m_idleKeys;
public:
	Index(const std::string& name, const std::string& ext) : File(name, ext), m_valueSize(0), m_keyLength(0), m_unitSize(0), m_blockSize(0) {
//		assert(MAX_KEY_LENGTH < 256 && "too large key length");
	}
	virtual ~Index(void){
		closeDB();
	}
	inline int set(uint64 key, const _TYPE_& value, bool setNotExist){
		// 查找是否有老数据，覆盖处理
		KeyValueMap& kvMap = getKeyValueMap();
		typename KeyValueMap::iterator itCur = kvMap.find(key);
		if(itCur != kvMap.end()){
			if(setNotExist){
				return FERR_KEY_ALREADY_EXIST;
			}
			if(!saveData(&value, sizeof(_TYPE_), itCur->second.offset, 0, false)){
				return FERR_KEY_SET_FAILED;
			}
			itCur->second.value = value;
			return FILE_OK;
		}
		// 保存新的节点数据
		IndexStorage keyS;
		keyS.value = value;
		keyS.setKey(key);
		int64 offset;
		bool isFromIdle;
		OffsetVector& idleKeys = m_idleKeys;
		if(idleKeys.empty()){
			offset = m_fileLength;
			isFromIdle = false;
		}else{
			offset = idleKeys.back();
			isFromIdle = true;
		}
		if(!saveData(&keyS, sizeof(IndexStorage), offset, 0, false)){
			return FERR_KEY_SET_FAILED;
		}
		if(isFromIdle){
			idleKeys.pop_back();
		}
		kvMap.insert(std::make_pair(key, KeyValue(value, offset)));
		return FILE_OK;
	}
	inline int get(uint64 key, _TYPE_& value){
		KeyValueMap& kvMap = getKeyValueMap();
		typename KeyValueMap::iterator itCur = kvMap.find(key);
		if(itCur == kvMap.end()){
			return FERR_KEY_NOT_FOUND;
		}
		value = itCur->second.value;
		return FILE_OK;
	}
	inline int get(uint64 key, _TYPE_** value){
		KeyValueMap& kvMap = getKeyValueMap();
		typename KeyValueMap::iterator itCur = kvMap.find(key);
		if(itCur == kvMap.end()){
			return FERR_KEY_NOT_FOUND;
		}
		(*value) = &(itCur->second.value);
		return FILE_OK;
	}
	inline int del(uint64 key, _TYPE_& value){
		KeyValueMap& kvMap = getKeyValueMap();
		typename KeyValueMap::iterator itCur = kvMap.find(key);
		if(itCur == kvMap.end()){
			return FERR_KEY_NOT_FOUND;
		}
		IndexStorage keyS;
		keyS.value = 0;
		keyS.setKey(0);
		int64 offset = itCur->second.offset;
		if(!saveData(&keyS, sizeof(IndexStorage), offset, 0, false)){
			return FERR_KEY_SET_FAILED;
		}
		value = itCur->second.value;
		OffsetVector& idleKeys = m_idleKeys;
		idleKeys.push_back(itCur->second.offset);
		kvMap.erase(itCur);
		return FILE_OK;
	}
	inline int incrby(uint64 key, _TYPE_& value){
		_TYPE_ old;
		int result = get(key, old);
		if(FERR_KEY_NOT_FOUND == result){
			return set(key, value, false);
		}else if(FILE_OK == result){
			value += old;
			return set(key, value, false);
		}else{
			return result;
		}
		return FILE_OK;
	}
	inline int replace(uint64 key, uint64 newKey){
		KeyValueMap& kvMapOld = getKeyValueMap();
		typename KeyValueMap::iterator itCur = kvMapOld.find(key);
		if(itCur == kvMapOld.end()){
			return FERR_KEY_NOT_FOUND;
		}
		KeyValueMap& kvMapNew = getKeyValueMap();
		typename KeyValueMap::iterator checkItCur = kvMapNew.find(newKey);
		if(checkItCur != kvMapNew.end()){
			return FERR_KEY_ALREADY_EXIST;
		}
		IndexStorage keyS;
		keyS.setKey(newKey);
        int64 offset = itCur->second.offset + sizeof(_TYPE_);
        if(!saveData(&(keyS.key), sizeof(uint64), offset, 0, false)){
            return FERR_KEY_SET_FAILED;
        }
		kvMapOld.erase(itCur);
		kvMapNew.insert(std::make_pair(newKey, itCur->second));
		return FILE_OK;
	}
	int openDB(void){
		// 尝试读取或创建文件
		int result = touchFile(NULL, 0);
		if(FILE_OK != result){
			return result;
		}
		// 这里需要重新打开文件
		if(!openReadWrite("rb+")){
			fprintf(stderr, "Index openDB failed openReadWrite rb+\n");
			return FERR_OPENRW_FAILED;
		}
		// 检查是否是第一次创建文件
		if(0 == m_fileLength){
//			fprintf(stderr, "Index open new DB file=%s\n", m_fileName.c_str());
			result = initializeDB();
			if(FILE_OK != result){
				closeDB();
				fprintf(stderr, "initializeDB failed\n");
				return result;
			}
		}else{
//			fprintf(stderr, "Index open DB from file=%s\n", m_fileName.c_str());
			result = initializeFromFile();
			if(FILE_OK != result){
				closeDB();
				fprintf(stderr, "initializeFromFile failed\n");
				return result;
			}
		}
		return FILE_OK;
	}
	void closeDB(void){
#ifdef USE_STREAM_FILE
		if(NULL != m_pFile){
			flush();
		}
#endif
		closeReadWrite();
	}
	void getNotEmptyValues(NodeVector& vec){
		_TYPE_ zero(0);
        for(auto &kv : m_keyMapArray){
            if(kv.second.value != zero){
                vec.push_back(kv.second.value);
            }
        }
	}
protected:
	inline KeyValueMap& getKeyValueMap(void){
		return m_keyMapArray;
	}
	int initializeDB(void){
		// 写入数据库的头部数据
		m_valueSize = sizeof(_TYPE_);
		m_keyLength = MAX_INDEX_KEY_LENGTH;
		m_unitSize = sizeof(IndexStorage);
		m_blockSize = BLOCK_SIZE;
		char temp[INDEX_HEAD_OFFSET];
		memcpy(temp, &m_valueSize, sizeof(uint64));
		memcpy(temp + sizeof(uint64), &m_keyLength, sizeof(uint64));
		memcpy(temp + sizeof(uint64)*2, &m_unitSize, sizeof(uint64));
		memcpy(temp + sizeof(uint64)*3, &m_blockSize, sizeof(uint64));
		if(INDEX_HEAD_OFFSET != seekWrite(temp, 1, INDEX_HEAD_OFFSET, 0, SEEK_SET)){
			fprintf(stderr, "Index::initializeDB write head failed\n");
			return FERR_INIT_WRITE_FAILED;
		}
		m_fileLength = writeTell();
		return FILE_OK;
	}
	int initializeFromFile(void){
		// 读取数据库头部数据
		seekRead(&(m_valueSize), 1, INDEX_HEAD_OFFSET, 0, SEEK_SET);
		if(m_valueSize != sizeof(_TYPE_)){
			fprintf(stderr, "Index::initializeFromFile m_valueSize=%lld \n", m_valueSize);
			return FERR_KEY_VALUE_SIZE_NOT_MATCH;
		}
		if(m_keyLength > MAX_INDEX_KEY_LENGTH){
			fprintf(stderr, "Index::initializeFromFile m_keyLength=%lld \n", m_keyLength);
			return FERR_KEY_LENGTH_NOT_MATCH;
		}
		if(m_unitSize != sizeof(IndexStorage)){
			fprintf(stderr, "Index::initializeFromFile m_unitSize=%lld \n", m_unitSize);
			return FERR_UNIT_SIZE_NOT_MATCH;
		}
		if(m_blockSize != BLOCK_SIZE){
			fprintf(stderr, "Index::initializeFromFile m_blockSize=%lld \n", m_blockSize);
			return FERR_BLOCK_SIZE_NOT_MATCH;
		}
		// 读取key数据
		int64 initSize = 100000;
		int64 tempBufferSize = sizeof(IndexStorage)*initSize;
		char* tempBuffer = new char[tempBufferSize];
		int64 fileLength = m_fileLength;
		int64 parseLength;
		int64 offset = INDEX_HEAD_OFFSET;	// 第一个key开始的位置
		fileLength -= offset;
		while (fileLength > tempBufferSize) {
			fileSeek(offset, SEEK_SET);
			fileRead(tempBuffer, 1, tempBufferSize);
			parseLength = initializeKey(tempBuffer, tempBufferSize, offset);
			fileLength -= parseLength;
		}
		if(fileLength > 0){
			fileSeek(offset, SEEK_SET);
			fileRead(tempBuffer, 1, fileLength);
			initializeKey(tempBuffer, fileLength, offset);
		}
		delete []tempBuffer;
		return FILE_OK;
	}
	int64 initializeKey(char* pBuffer, int64 bufferSize, int64& offset){
		int64 parseLength = bufferSize;
		_TYPE_ zero(0);
		while (bufferSize >= sizeof(IndexStorage) ) {
		    IndexStorage* pKey = (IndexStorage*)pBuffer;
		    if(pKey->value == zero){
		        m_idleKeys.push_back(offset);
		    }else{
                KeyValueMap& kvMap = getKeyValueMap();
				kvMap.insert(std::make_pair(pKey->key, KeyValue(pKey->value, offset)));
		    }
			offset += sizeof(IndexStorage);
			pBuffer += sizeof(IndexStorage);
			bufferSize -= sizeof(IndexStorage);
		}
		parseLength -= bufferSize;
		return parseLength;
	}
};


NS_HIVE_END

#endif
