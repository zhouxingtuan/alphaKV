//
//  keyvalue.hpp
//  base
//
//  Created by AppleTree on 16/12/25.
//  Copyright © 2016年 AppleTree. All rights reserved.
//

#ifndef keyvalue_hpp
#define keyvalue_hpp

#include "key.hpp"
#include "index.hpp"
#include "idle.hpp"

NS_HIVE_BEGIN

typedef std::vector<char> CharVector;

// 同时作为偏移和空闲块的结构
typedef struct BlockNode{
	union{
		struct{
			uint64 offset		: 40;	// 文件块的偏移（块下标）
			uint64 size			: 24;	// 连续文件块的数量
		};
		uint64 value;
	};
	BlockNode(uint64 o, uint64 s) : offset(o), size(s){}
	BlockNode(uint64 v) : value(v){}
	BlockNode(void) : value(0) {}
	inline BlockNode& operator=(uint64 v){ this->value = v; return *this; }
	inline BlockNode& operator=(const BlockNode& other){ this->value = other.value; return *this; }
	inline bool operator==(uint64 v) const { return (v == this->value); }
	inline bool operator==(const BlockNode& other) const { return (other.value == this->value); }
	inline bool operator!=(uint64 v) const { return (v != this->value); }
	inline bool operator!=(const BlockNode& other) const { return (other.value != this->value); }
}BlockNode;

template <uint64 _KEY_SLOT_NUMBER_>
class KeyValue : public File
{
public:
	typedef BlockNode _TYPE_;
	typedef std::vector<_TYPE_> NodeVector;
	typedef Key<_TYPE_, _KEY_SLOT_NUMBER_> KeyMap;
	typedef Index<_TYPE_> IndexMap;
	typedef Idle<_TYPE_> IdleNode;
	KeyMap* m_pKeyOffset;					// key对应的偏移值文件
	IndexMap* m_pIndexOffset;               // 数字key对应的偏移文件
	IdleNode m_idles;
public:
	KeyValue(const std::string& name) : File(name, ".v") {
		m_pKeyOffset = new KeyMap(name, ".k");
		m_pIndexOffset = new IndexMap(name, ".i");
	}
	virtual ~KeyValue(void){
		closeDB();
	}
	// recordLength 是否记录四个字节(int)的数据长度
	// setNotExist 为true时，如果已经存在，就直接返回错误
	inline int set(const char* key, int64 keyLen, const void* value, int64 valueLen, bool recordLength, bool setNotExist){
		int64 saveLength;
		if(recordLength){
			saveLength = valueLen + 4;
		}else{
			saveLength = valueLen;
		}
		// 查找原先是否存在这个index的数据
		uint64 blockSize = getBlockSize(saveLength);
		// 检查数据块是否太大
		if(blockSize > BLOCK_MAX_SAVE_NUMBER){
			return FERR_BLOCK_TOO_LARGE;
		}
		_TYPE_ node;
		int result = m_pKeyOffset->get(key, keyLen, node);
		if(result != FILE_OK || node.size == 0){
			// 获取一个空闲的存储节点来保存数据
			uint64 idleIndex, blockOffset;
			_TYPE_* pIdleNode = m_idles.getIdleNode(blockSize, &idleIndex);
			// 没有空闲的存储节点，就保存到文件的末尾
			if(NULL == pIdleNode){
				blockOffset = getBlockOffsetAtEnd();
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				return m_pKeyOffset->set(key, keyLen, _TYPE_(blockOffset, blockSize), false);
			}else{
				blockOffset = pIdleNode->offset;
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				result = m_pKeyOffset->set(key, keyLen, _TYPE_(blockOffset, blockSize), false);
				if(result != FILE_OK){
					return result;
				}
				m_idles.useIdleNode(pIdleNode, idleIndex, blockSize);
				return FILE_OK;
			}
		}
		if(setNotExist){
			return FERR_KEY_ALREADY_EXIST;
		}
		// 如果数据块更改，那么需要为数据块寻找新的存储位置；同时，修改index下面该数据记录的占用数据块offset和size
		uint64 nodeOffset = node.offset;
		uint64 nodeSize = node.size;
		if(blockSize != nodeSize){
			// 获取一个空闲的存储节点来保存数据
			uint64 idleIndex, blockOffset;
			_TYPE_* pIdleNode = m_idles.getIdleNode(blockSize, &idleIndex);
			// 没有空闲的存储节点，就保存到文件的末尾
			if(NULL == pIdleNode){
				blockOffset = getBlockOffsetAtEnd();
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				result = m_pKeyOffset->set(key, keyLen, _TYPE_(blockOffset, blockSize), false);
				if(result != FILE_OK){
					return result;
				}
			}else{
				blockOffset = pIdleNode->offset;
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				result = m_pKeyOffset->set(key, keyLen, _TYPE_(blockOffset, blockSize), false);
				if(result != FILE_OK){
					return result;
				}
				m_idles.useIdleNode(pIdleNode, idleIndex, blockSize);
			}
			// 原先保存的位置将作为新的空闲数据加入
			m_idles.setIdleNode(nodeOffset, nodeSize);
			
			return FILE_OK;
		}else{
			// 直接保存内容到原来的偏移位置
			int64 offset = nodeOffset * BLOCK_SIZE;
			if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
				return FERR_BLOCK_SET_FAILED;
			}
		}
		return FILE_OK;
	}
	inline int get(const char* key, int64 keyLen, CharVector& data){
		int result;
		_TYPE_ node;
		result = m_pKeyOffset->get(key, keyLen, node);
		if(result != FILE_OK){
			return result;
		}
		uint64 nodeSize = node.size;
		if(nodeSize == 0){
			return FERR_BLOCK_EMPTY;
		}
		uint64 nodeOffset = node.offset;
		int64 saveOffset = nodeOffset * BLOCK_SIZE;
		int64 saveLength = nodeSize * BLOCK_SIZE;
		data.resize(saveLength, 0);
		if(saveLength != seekRead(data.data(), 1, saveLength, saveOffset, SEEK_SET)){
			return FERR_BLOCK_READ_FAIL;
		}
		return FILE_OK;
	}
	inline int del(const char* key, int64 keyLen){
		_TYPE_ node;
		int result;
		result = m_pKeyOffset->del(key, keyLen, node);
		if(result != FILE_OK){
			return result;
		}
		// 原先保存的位置将作为新的空闲数据加入
		m_idles.setIdleNode(node.offset, node.size);
		return FILE_OK;
	}
	inline int replace(const char* key, uint64 length, const char* newKey, uint64 newLength){
		return m_pKeyOffset->replace(key, length, newKey, newLength);
	}
	// apis for number key -> value
	inline int set(uint64 key, const void* value, int64 valueLen, bool recordLength, bool setNotExist){
		int64 saveLength;
		if(recordLength){
			saveLength = valueLen + 4;
		}else{
			saveLength = valueLen;
		}
		// 查找原先是否存在这个index的数据
		uint64 blockSize = getBlockSize(saveLength);
		// 检查数据块是否太大
		if(blockSize > BLOCK_MAX_SAVE_NUMBER){
			return FERR_BLOCK_TOO_LARGE;
		}
		_TYPE_ node;
		int result = m_pIndexOffset->get(key, node);
		if(result != FILE_OK || node.size == 0){
			// 获取一个空闲的存储节点来保存数据
			uint64 idleIndex, blockOffset;
			_TYPE_* pIdleNode = m_idles.getIdleNode(blockSize, &idleIndex);
			// 没有空闲的存储节点，就保存到文件的末尾
			if(NULL == pIdleNode){
				blockOffset = getBlockOffsetAtEnd();
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				return m_pIndexOffset->set(key, _TYPE_(blockOffset, blockSize), false);
			}else{
				blockOffset = pIdleNode->offset;
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				result = m_pIndexOffset->set(key, _TYPE_(blockOffset, blockSize), false);
				if(result != FILE_OK){
					return result;
				}
				m_idles.useIdleNode(pIdleNode, idleIndex, blockSize);
				return FILE_OK;
			}
		}
		if(setNotExist){
			return FERR_KEY_ALREADY_EXIST;
		}
		// 如果数据块更改，那么需要为数据块寻找新的存储位置；同时，修改index下面该数据记录的占用数据块offset和size
		uint64 nodeOffset = node.offset;
		uint64 nodeSize = node.size;
		if(blockSize != nodeSize){
			// 获取一个空闲的存储节点来保存数据
			uint64 idleIndex, blockOffset;
			_TYPE_* pIdleNode = m_idles.getIdleNode(blockSize, &idleIndex);
			// 没有空闲的存储节点，就保存到文件的末尾
			if(NULL == pIdleNode){
				blockOffset = getBlockOffsetAtEnd();
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				result = m_pIndexOffset->set(key, _TYPE_(blockOffset, blockSize), false);
				if(result != FILE_OK){
					return result;
				}
			}else{
				blockOffset = pIdleNode->offset;
				int64 offset = blockOffset * BLOCK_SIZE;
				if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
					return FERR_BLOCK_SET_FAILED;
				}
				result = m_pIndexOffset->set(key, _TYPE_(blockOffset, blockSize), false);
				if(result != FILE_OK){
					return result;
				}
				m_idles.useIdleNode(pIdleNode, idleIndex, blockSize);
			}
			// 原先保存的位置将作为新的空闲数据加入
			m_idles.setIdleNode(nodeOffset, nodeSize);

			return FILE_OK;
		}else{
			// 直接保存内容到原来的偏移位置
			int64 offset = nodeOffset * BLOCK_SIZE;
			if(!saveData(value, valueLen, offset, BLOCK_SIZE, recordLength)){
				return FERR_BLOCK_SET_FAILED;
			}
		}
		return FILE_OK;
	}
	inline int get(uint64 key, CharVector& data){
		int result;
		_TYPE_ node;
		result = m_pIndexOffset->get(key, node);
		if(result != FILE_OK){
			return result;
		}
		uint64 nodeSize = node.size;
		if(nodeSize == 0){
			return FERR_BLOCK_EMPTY;
		}
		uint64 nodeOffset = node.offset;
		int64 saveOffset = nodeOffset * BLOCK_SIZE;
		int64 saveLength = nodeSize * BLOCK_SIZE;
		data.resize(saveLength, 0);
		if(saveLength != seekRead(data.data(), 1, saveLength, saveOffset, SEEK_SET)){
			return FERR_BLOCK_READ_FAIL;
		}
		return FILE_OK;
	}
	inline int del(uint64 key){
		_TYPE_ node;
		int result;
		result = m_pIndexOffset->del(key, node);
		if(result != FILE_OK){
			return result;
		}
		// 原先保存的位置将作为新的空闲数据加入
		m_idles.setIdleNode(node.offset, node.size);
		return FILE_OK;
	}
	inline int replace(uint64 key, uint64 newKey){
	    return m_pIndexOffset->replace(key, newKey);
	}
protected:
	inline int64 getBlockSize(int64 length){
		uint64 blockSize;
		blockSize = length / BLOCK_SIZE;
		if(length % BLOCK_SIZE != 0){
			++blockSize;
		}
		return blockSize;
	}
	inline uint64 getBlockOffsetAtEnd(void) const {
		return (m_fileLength / BLOCK_SIZE);
	}
public:
	static bool compareNodeOffset(const _TYPE_& a, const _TYPE_& b){
		return (a.offset < b.offset);
	}
	static bool compareNodeSize(const _TYPE_& a, const _TYPE_& b){
		return (a.size < b.size);
	}
	int openDB(void){
		int result;
		// 尝试创建Index的文件
		result = m_pKeyOffset->openDB();
		if(FILE_OK != result){
			return result;
		}
		result = m_pIndexOffset->openDB();
		if(FILE_OK != result){
			return result;
		}
		// 尝试读取或创建文件
		result = touchFile(NULL, 0);
		if(FILE_OK != result){
			return result;
		}
		// 这里需要重新打开文件
		if(!openReadWrite("rb+")){
			fprintf(stderr, "Array openDB failed openReadWrite rb+\n");
			return FERR_OPENRW_FAILED;
		}
		// 计算空闲数据块：将index数据按照offset从小到大排序，依次统计中间缺失的数据，该数据为空闲数据
		NodeVector dataNode;
		m_pKeyOffset->getNotEmptyValues(dataNode);
		m_pIndexOffset->getNotEmptyValues(dataNode);
		if( dataNode.size() > 0 ){
			std::sort(dataNode.begin(), dataNode.end(), compareNodeOffset);
			// 检查文件头部到第一个数据节点间的空闲数据块
			if(dataNode.front().offset > 0){
				m_idles.addIdleNodeAtEnd(0, dataNode.front().offset);
			}
			// 计算idle数据，两个数据节点之间为空闲数据块
			uint64 offset, size;
			for(size_t i=1; i<dataNode.size(); ++i){
				_TYPE_& preNode = dataNode[i-1];
				_TYPE_& curNode = dataNode[i];
				offset = preNode.offset + preNode.size;
				if(curNode.offset > offset){
					size = curNode.offset - offset;
					// 检查idle节点保存的数量是不是超过了长度，需要分开成多个节点
					m_idles.addIdleNodeAtEnd(offset, size);
				}
			}
			// 检查文件末尾到最后一个数据节点的空闲数据
			offset = dataNode.back().offset + dataNode.back().size;
			uint64 fileEndOffset = m_fileLength / BLOCK_SIZE;
			if(fileEndOffset > offset){
				size = fileEndOffset - offset;
				m_idles.addIdleNodeAtEnd(offset, size);
			}
		}else{
			uint64 fileEndOffset = m_fileLength / BLOCK_SIZE;
			if(fileEndOffset > 0){
				m_idles.addIdleNodeAtEnd(0, fileEndOffset);
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
		if(NULL != m_pKeyOffset){
#ifdef USE_STREAM_FILE
			m_pKeyOffset->flush();
#endif
			delete m_pKeyOffset;
			m_pKeyOffset = NULL;
		}
		if(NULL != m_pIndexOffset){
#ifdef USE_STREAM_FILE
			m_pIndexOffset->flush();
#endif
			delete m_pIndexOffset;
			m_pIndexOffset = NULL;
		}
		closeReadWrite();
	}

};

NS_HIVE_END

#endif /* keyvalue_hpp */





