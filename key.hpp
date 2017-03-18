//
//  key.hpp
//  base
//
//  Created by AppleTree on 16/12/25.
//  Copyright © 2016年 AppleTree. All rights reserved.
//

#ifndef key_hpp
#define key_hpp

#include "file.hpp"

NS_HIVE_BEGIN

#define BINARY_HASH_SEED 5381

#ifndef uint64_t
typedef uint64_t uint64;
#endif

inline uint64_t MurmurHash64A ( const void * key, int len, unsigned int seed )
{
	const uint64_t m = 0xc6a4a7935bd1e995;
	const int r = 47;
	
	uint64_t h = seed ^ (len * m);
	
	const uint64_t * data = (const uint64_t *)key;
	const uint64_t * end = data + (len/8);
	
	while(data != end)
	{
		uint64_t k = *data++;
		
		k *= m;
		k ^= k >> r;
		k *= m;
		
		h ^= k;
		h *= m;
	}
	
	const unsigned char * data2 = (const unsigned char*)data;
	
	switch(len & 7)
	{
		case 7: h ^= uint64_t(data2[6]) << 48;
		case 6: h ^= uint64_t(data2[5]) << 40;
		case 5: h ^= uint64_t(data2[4]) << 32;
		case 4: h ^= uint64_t(data2[3]) << 24;
		case 3: h ^= uint64_t(data2[2]) << 16;
		case 2: h ^= uint64_t(data2[1]) << 8;
		case 1: h ^= uint64_t(data2[0]);
			h *= m;
	};
	
	h ^= h >> r;
	h *= m;
	h ^= h >> r;
	
	return h;
}
// 64-bit hash for 32-bit platforms
inline uint64_t MurmurHash64B ( const void * key, int len, unsigned int seed )
{
	const unsigned int m = 0x5bd1e995;
	const int r = 24;
	
	unsigned int h1 = seed ^ len;
	unsigned int h2 = 0;
	
	const unsigned int * data = (const unsigned int *)key;
	
	while(len >= 8)
	{
		unsigned int k1 = *data++;
		k1 *= m; k1 ^= k1 >> r; k1 *= m;
		h1 *= m; h1 ^= k1;
		len -= 4;
		
		unsigned int k2 = *data++;
		k2 *= m; k2 ^= k2 >> r; k2 *= m;
		h2 *= m; h2 ^= k2;
		len -= 4;
	}
	
	if(len >= 4)
	{
		unsigned int k1 = *data++;
		k1 *= m; k1 ^= k1 >> r; k1 *= m;
		h1 *= m; h1 ^= k1;
		len -= 4;
	}
	
	switch(len)
	{
		case 3: h2 ^= ((unsigned char*)data)[2] << 16;
		case 2: h2 ^= ((unsigned char*)data)[1] << 8;
		case 1: h2 ^= ((unsigned char*)data)[0];
			h2 *= m;
	};
	
	h1 ^= h2 >> 18; h1 *= m;
	h2 ^= h1 >> 22; h2 *= m;
	h1 ^= h2 >> 17; h1 *= m;
	h2 ^= h1 >> 19; h2 *= m;
	
	uint64_t h = h1;
	
	h = (h << 32) | h2;
	
	return h;
}

#ifdef __APPLE__
#define binary_hash MurmurHash64A
#else
#define binary_hash MurmurHash64B
#endif

#define KEY_HEAD_OFFSET 32
#define MAX_KEY_LENGTH 256

template <typename _TYPE_, uint64 _KEY_SLOT_NUMBER_>
class Key : public File
{
public:
	typedef struct KeyStorage {
		_TYPE_ value;					// value的数值
		char key[MAX_KEY_LENGTH];		// key的值
		inline uint8 getKeyLength(void) const { return (uint8)(key[0]); }
		inline void setKeyLength(uint8 length) { (*(uint8*)key) = length; }
		inline uint8 getEmptyLength(void) const { return (uint8)(key[1]); }
		inline void setEmptyLength(uint8 length) { (*(uint8*)(key[1])) = length; }
		inline void setKey(const char* ptr, uint8 length){
			setKeyLength(length);
			memcpy(key + 1, ptr, length);
		}
	} KeyStorage;
	typedef struct KeyValue {
		_TYPE_ value;
		int64 offset;
		KeyValue(_TYPE_ value, int64 offset) : value(value), offset(offset){}
		KeyValue(void) : value(0), offset(0) {}
		KeyValue(const KeyValue& other) : value(other.value), offset(other.offset) {}
		inline KeyValue& operator=(const KeyValue& other){ this->value = other.value; this->offset = other.offset; return *this; }
	}KeyValue;
//	typedef std::map<std::string, KeyValue> KeyValueMap;
	typedef std::unordered_map<std::string, KeyValue> KeyValueMap;
	typedef std::vector<_TYPE_> NodeVector;
	typedef std::vector<int64> OffsetVector;
	typedef std::vector<OffsetVector> OffsetVectorArray;
	
	uint64 m_valueSize;					// 保存value的长度
	uint64 m_keyLength;					// key的长度上限
	uint64 m_unitSize;					// key存储单元的长度
	uint64 m_blockSize;					// data存储单元的长度
	KeyValueMap m_keyMapArray[_KEY_SLOT_NUMBER_];
	OffsetVector m_idleKeysArray[MAX_KEY_LENGTH];
//	OffsetVector m_idleKeys;
public:
	Key(const std::string& name, const std::string& ext) : File(name, ext), m_valueSize(0), m_keyLength(0), m_unitSize(0), m_blockSize(0) {
//		assert(MAX_KEY_LENGTH < 256 && "too large key length");
	}
	virtual ~Key(void){
		closeDB();
	}
	inline int set(const char* key, uint64 length, const _TYPE_& value, bool setNotExist){
		if(length >= MAX_KEY_LENGTH){
			return FERR_KEY_IS_TOO_LONG;
		}
		// 查找是否有老数据，覆盖处理
		std::string keyString(key, length);
		KeyValueMap& kvMap = findKeyValueMap(key, length);
		typename KeyValueMap::iterator itCur = kvMap.find(keyString);
		if(itCur != kvMap.end()){
			if(setNotExist){
				return FERR_KEY_ALREADY_EXIST;
			}
			if(!saveData(&value, sizeof(_TYPE_), itCur->second.offset, 0, false)){
				return FERR_KEY_SET_FAILED;
			}
			return FILE_OK;
		}
		// 保存新的节点数据
		KeyStorage keyS;
		keyS.value = value;
		keyS.setKey(key, (uint8)length);
		int64 offset;
		bool isFromIdle;
		OffsetVector& idleKeys = m_idleKeysArray[length];
		if(idleKeys.empty()){
			offset = m_fileLength;
			isFromIdle = false;
		}else{
			offset = idleKeys.back();
			isFromIdle = true;
		}
		if(!saveData(&keyS, sizeof(_TYPE_) + 1 + length, offset, 0, false)){
			return FERR_KEY_SET_FAILED;
		}
		if(isFromIdle){
			idleKeys.pop_back();
		}
		kvMap.insert(std::make_pair(keyString, KeyValue(value, offset)));
		return FILE_OK;
	}
	inline int get(const char* key, uint64 length, _TYPE_& value){
		KeyValueMap& kvMap = findKeyValueMap(key, length);
		typename KeyValueMap::iterator itCur = kvMap.find(std::string(key, length));
		if(itCur == kvMap.end()){
			return FERR_KEY_NOT_FOUND;
		}
		value = itCur->second.value;
		return FILE_OK;
	}
	inline int get(const char* key, uint64 length, _TYPE_** value){
		KeyValueMap& kvMap = findKeyValueMap(key, length);
		typename KeyValueMap::iterator itCur = kvMap.find(std::string(key, length));
		if(itCur == kvMap.end()){
			return FERR_KEY_NOT_FOUND;
		}
		(*value) = &(itCur->second.value);
		return FILE_OK;
	}
	inline int del(const char* key, uint64 length, _TYPE_& value){
		KeyValueMap& kvMap = findKeyValueMap(key, length);
		typename KeyValueMap::iterator itCur = kvMap.find(std::string(key, length));
		if(itCur == kvMap.end()){
			return FERR_KEY_NOT_FOUND;
		}
		KeyStorage keyS;
		keyS.setEmptyLength(keyS.getKeyLength());	// key[1] 保存原始长度
		keyS.value = 0;
		keyS.setKeyLength(0);						// key[0] == 0 表示idle状态
		int64 offset = itCur->second.offset;
		if(!saveData(&keyS, sizeof(_TYPE_) + 2, offset, 0, false)){
			return FERR_KEY_SET_FAILED;
		}
		value = itCur->second.value;
		OffsetVector& idleKeys = m_idleKeysArray[length];
		idleKeys.push_back(itCur->second.offset);
		kvMap.erase(itCur);
		return FILE_OK;
	}
	inline int incrby(const char* key, uint64 length, _TYPE_& value){
		_TYPE_ old;
		int result = get(key, length, old);
		if(FERR_KEY_NOT_FOUND == result){
			return set(key, length, value, false);
		}else if(FILE_OK == result){
			value += old;
			return set(key, length, value, false);
		}else{
			return result;
		}
		return FILE_OK;
	}
	inline int replace(const char* key, uint64 length, const char* newKey, uint64 newLength){
		if(newLength >= MAX_KEY_LENGTH){
			return FERR_KEY_IS_TOO_LONG;
		}
		KeyValueMap& kvMapOld = findKeyValueMap(key, length);
		typename KeyValueMap::iterator itCur = kvMapOld.find(std::string(key, length));
		if(itCur == kvMapOld.end()){
			return FERR_KEY_NOT_FOUND;
		}
		std::string newKeyString(newKey, newLength);
		KeyValueMap& kvMapNew = findKeyValueMap(newKey, newLength);
		typename KeyValueMap::iterator checkItCur = kvMapNew.find(newKeyString);
		if(checkItCur != kvMapNew.end()){
			return FERR_KEY_ALREADY_EXIST;
		}
		KeyStorage keyS;
		keyS.setKey(newKey, (uint8)newLength);
		if(length == newLength){
			int64 offset = itCur->second.offset + sizeof(_TYPE_);
			int64 saveLength = newLength + 1;
			if(!saveData(keyS.key, saveLength, offset, 0, false)){
				return FERR_KEY_SET_FAILED;
			}
		}else{
			// 获取新的长度保存
			int64 offset;
			bool isFromIdle;
			OffsetVector& idleKeys = m_idleKeysArray[newLength];
			if(idleKeys.empty()){
				offset = m_fileLength;
				isFromIdle = false;
			}else{
				offset = idleKeys.back();
				isFromIdle = true;
			}
			if(!saveData(&keyS, sizeof(_TYPE_) + 1 + newLength, offset, 0, false)){
				return FERR_KEY_SET_FAILED;
			}
			if(isFromIdle){
				idleKeys.pop_back();
			}
			// 回收旧的key空间
			OffsetVector& idleKeysOld = m_idleKeysArray[length];
			idleKeysOld.push_back(itCur->second.offset);
		}
		kvMapOld.erase(itCur);
		kvMapNew.insert(std::make_pair(newKeyString, itCur->second));
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
			fprintf(stderr, "Key openDB failed openReadWrite rb+\n");
			return FERR_OPENRW_FAILED;
		}
		// 检查是否是第一次创建文件
		if(0 == m_fileLength){
//			fprintf(stderr, "Key open new DB file=%s\n", m_fileName.c_str());
			result = initializeDB();
			if(FILE_OK != result){
				closeDB();
				fprintf(stderr, "initializeDB failed\n");
				return result;
			}
		}else{
//			fprintf(stderr, "Key open DB from file=%s\n", m_fileName.c_str());
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
		for(uint64 index = 0; index < _KEY_SLOT_NUMBER_; ++index){
			KeyValueMap& kvMap = m_keyMapArray[index];
			for(auto &kv : kvMap){
				if(kv.second.value != zero){
					vec.push_back(kv.second.value);
				}
			}
		}
	}
protected:
	inline KeyValueMap& findKeyValueMap(const char* key, uint64 length){
		uint64 hash = binary_hash(key, (int)length, BINARY_HASH_SEED);
		int index = hash % _KEY_SLOT_NUMBER_;
		return m_keyMapArray[index];
	}
	int initializeDB(void){
		// 写入数据库的头部数据
		m_valueSize = sizeof(_TYPE_);
		m_keyLength = MAX_KEY_LENGTH;
		m_unitSize = sizeof(KeyStorage);
		m_blockSize = BLOCK_SIZE;
		char temp[KEY_HEAD_OFFSET];
		memcpy(temp, &m_valueSize, sizeof(uint64));
		memcpy(temp + sizeof(uint64), &m_keyLength, sizeof(uint64));
		memcpy(temp + sizeof(uint64)*2, &m_unitSize, sizeof(uint64));
		memcpy(temp + sizeof(uint64)*3, &m_blockSize, sizeof(uint64));
		if(KEY_HEAD_OFFSET != seekWrite(temp, 1, KEY_HEAD_OFFSET, 0, SEEK_SET)){
			fprintf(stderr, "Key::initializeDB write head failed\n");
			return FERR_INIT_WRITE_FAILED;
		}
		m_fileLength = writeTell();
		return FILE_OK;
	}
	int initializeFromFile(void){
		// 读取数据库头部数据
		seekRead(&(m_valueSize), 1, KEY_HEAD_OFFSET, 0, SEEK_SET);
		if(m_valueSize != sizeof(_TYPE_)){
			fprintf(stderr, "Key::initializeFromFile m_valueSize=%lld \n", m_valueSize);
			return FERR_KEY_VALUE_SIZE_NOT_MATCH;
		}
		if(m_keyLength > MAX_KEY_LENGTH){
			fprintf(stderr, "Key::initializeFromFile m_keyLength=%lld \n", m_keyLength);
			return FERR_KEY_LENGTH_NOT_MATCH;
		}
		if(m_unitSize != sizeof(KeyStorage)){
			fprintf(stderr, "Key::initializeFromFile m_unitSize=%lld \n", m_unitSize);
			return FERR_UNIT_SIZE_NOT_MATCH;
		}
		if(m_blockSize != BLOCK_SIZE){
			fprintf(stderr, "Key::initializeFromFile m_blockSize=%lld \n", m_blockSize);
			return FERR_BLOCK_SIZE_NOT_MATCH;
		}
		// 读取key数据
		int64 initSize = 100000;
		int64 tempBufferSize = sizeof(KeyStorage)*initSize;
		char* tempBuffer = new char[tempBufferSize];
		int64 fileLength = m_fileLength;
		int64 parseLength;
		int64 offset = KEY_HEAD_OFFSET;	// 第一个key开始的位置
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
		int keyLength;
		uint8 length;
		uint8 emptyLength;
		uint8 atLeastLength = sizeof(_TYPE_) + 2;
		uint8 emptyLengthIndex = sizeof(_TYPE_) + 1;
		while (bufferSize >= atLeastLength ) {
			length = pBuffer[sizeof(_TYPE_)];
			if(0 == length){
				emptyLength = pBuffer[emptyLengthIndex];
				keyLength = sizeof(_TYPE_) + 1 + emptyLength;
				if(keyLength > bufferSize){
					break;
				}
				OffsetVector& idleKeys = m_idleKeysArray[emptyLength];
				idleKeys.push_back(offset);
			}else{
				keyLength = sizeof(_TYPE_) + 1 + length;
				if(keyLength > bufferSize){
					break;
				}
				std::string key(pBuffer + emptyLengthIndex, length);
				KeyValueMap& kvMap = findKeyValueMap(key.c_str(), key.length());
				kvMap.insert(std::make_pair(key, KeyValue(*(_TYPE_*)(pBuffer), offset)));
			}
			offset += keyLength;
			pBuffer += keyLength;
			bufferSize -= keyLength;
		}
		parseLength -= bufferSize;
		return parseLength;
	}
};

NS_HIVE_END

#endif /* key_hpp */








