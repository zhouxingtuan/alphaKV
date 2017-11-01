//
//  idle.hpp
//  base
//
//  Created by AppleTree on 16/12/25.
//  Copyright © 2016年 AppleTree. All rights reserved.
//

#ifndef idle_hpp
#define idle_hpp

#include "file.hpp"

NS_HIVE_BEGIN

#define BLOCK_MAX_SAVE_NUMBER 524288	// 64M/128Byte计算的结果
#define BLOCK_MAX_SAVE_SIZE 67108864	// 64M，最大保存的单个文件块长度
#define BLOCK_MAX_IDLE_NUMBER 16777215	// 空闲块的数量最大值，超过会分成两个来保存,<2G

#define IDLE_LIMITED_LOOP 1024

template <typename _NODE_>
class Idle
{
public:
	typedef std::vector<_NODE_> IdleNodeVector;
	IdleNodeVector m_idles;			// 用于存储空闲节点的数组
	int64 m_maxIdleSize;			// 记录最大的Idle数据
	int64 m_maxIdleIndex;			// 记录最大的Idle的下标
	bool m_isMaxIdleNew;			// 当前的idle是不是最新的
public:
	Idle(void) : m_maxIdleSize(0), m_maxIdleIndex(0), m_isMaxIdleNew(false) {}
	virtual ~Idle(void){}
	// 找到能够保存数据的节点；没有则返回NULL
	inline _NODE_* getIdleNode(uint64 size, uint64* index){
		if(m_idles.empty()){
			return NULL;
		}
		// 优先查找上一个记录的最大的段
		if((uint64)m_maxIdleSize > size){
			*index = m_maxIdleIndex;
			return &m_idles[m_maxIdleIndex];
		}
		if(m_isMaxIdleNew){
			return NULL;
		}
		int64 arraySize = (int64)m_idles.size();
		if(arraySize > IDLE_LIMITED_LOOP){
			return NULL;
		}
		uint64 emptySize;
		for(int64 i = arraySize - 1; i > -1; --i){
			_NODE_ &node = m_idles[i];
			emptySize = node.size;
			if(emptySize > (uint64)m_maxIdleSize){
				m_maxIdleSize = emptySize;
				m_maxIdleIndex = i;
			}
		}
		m_isMaxIdleNew = true;
		if((uint64)m_maxIdleSize > size){
			*index = m_maxIdleIndex;
			return &m_idles[m_maxIdleIndex];
		}
		return NULL;
	}
	// 添加一个新的空闲数据信息
	inline void setIdleNode(uint64 offset, uint64 size){
		// 找到是否有相连的节点，直接拼接空闲节点
		if(m_idles.empty()){
			addIdleNodeAtEnd(offset, size);
			return;
		}
		// 检查是否加入到末尾
		{
			_NODE_& endNode = m_idles.back();
			uint64 endNodeEndOffset = getIdleNodeEndOffset(endNode);
			if(offset == endNodeEndOffset ){
				uint64 newSize = endNode.size + size;
				if(newSize > BLOCK_MAX_IDLE_NUMBER){
					addIdleNodeAtEnd(offset, size);
				}else{
					endNode.size = newSize;
				}
				return;
			}else if(offset > endNodeEndOffset){
				addIdleNodeAtEnd(offset, size);
				return;
			}
		}
		// 检查是否加入到头部
		{
			_NODE_& beginNode = m_idles.front();
			int64 endOffset = offset + size;
			int64 beginNodeOffset = beginNode.offset;
			if(endOffset == beginNodeOffset){
				uint64 newSize = beginNode.size + size;
				if(newSize > BLOCK_MAX_IDLE_NUMBER){
					addIdleNodeAtBegin(offset, size);
				}else{
					beginNode.size = newSize;
					beginNode.offset = offset;
				}
				return;
			}else if(endOffset < beginNodeOffset){
				addIdleNodeAtBegin(offset, size);
				return;
			}
		}
		// 没有就根据offset定位到相应的位置插入
		int64 min,mid,max;
		uint64 nodeOffset;
		max = m_idles.size() - 1;
		min = 0;
		mid = (min + max) / 2;
		do{
			_NODE_& node = m_idles[mid];
			nodeOffset = node.offset;
			if(offset < nodeOffset){
				// 判断二分是不是应该停止；这里返回的是min，属于前面的集合
				if(min + 1 == mid){
					max = mid;
					break;
				}
				max = mid;
				mid = (min + mid) / 2;
			}else{
				// 判断二分是不是应该停止；这里返回的是mid，属于mid所属的集合
				if(mid + 1 == max){
					min = mid;
					mid = max;
					break;
				}
				min = mid;
				mid = (mid + max) / 2;
			}
		}while(true);
		// 检查合并操作min和max
		int64 newSize;
		int opType = 0;
		_NODE_& minNode = m_idles[min];
		if(getIdleNodeEndOffset(minNode) == offset){
			opType = 1;
		}
		_NODE_& maxNode = m_idles[max];
		if(offset + size == maxNode.offset){
			if(opType == 1){
				opType = 3;
			}else{
				opType = 2;
			}
		}
		switch (opType) {
				// 没有合并操作
			case 0:{
				m_idles.insert(m_idles.begin() + max, _NODE_(offset, size));
				break;
			}
				// 和min节点合并
			case 1:{
				newSize = minNode.size + size;
				if(newSize > BLOCK_MAX_IDLE_NUMBER){
					m_idles.insert(m_idles.begin() + max, _NODE_(offset, size));
				}else{
					minNode.size = newSize;
				}
				break;
			}
				// 和max节点合并
			case 2:{
				newSize = maxNode.size + size;
				if(newSize > BLOCK_MAX_IDLE_NUMBER){
					m_idles.insert(m_idles.begin() + max, _NODE_(offset, size));
				}else{
					maxNode.size = newSize;
				}
				break;
			}
				// 三个节点合并检查
			case 3:{
				newSize = minNode.size + size;
				if(newSize > BLOCK_MAX_IDLE_NUMBER){
					newSize = maxNode.size + size;
					if(newSize > BLOCK_MAX_IDLE_NUMBER){	// 前后都不合并
						m_idles.insert(m_idles.begin() + max, _NODE_(offset, size));
					}else{									// 只合并到max节点
						maxNode.size = newSize;
					}
				}else{
					if(newSize + maxNode.size > BLOCK_MAX_IDLE_NUMBER){	// 只合并到min节点
						minNode.size = newSize;
					}else{									// 同时合并min和max节点；删除max节点
						minNode.size = newSize + maxNode.size;
						m_idles.erase(m_idles.begin() + max);
					}
				}
				break;
			}
			default:{
				// ...?
			}
		}
	}
	// 使用这个空闲节点
	inline void useIdleNode(_NODE_* pNode, uint64 index, uint64 size){
		m_isMaxIdleNew = false;
		uint64 emptySize = pNode->size;
		if(emptySize == size){
			m_idles.erase(m_idles.begin() + index);
			m_maxIdleSize = 0;
		}else if(emptySize > size){
			pNode->size -= size;
			m_maxIdleSize = pNode->size;
		}else{
			// ? error must be happened
			fprintf(stderr, "Why we come here?\n");
		}
	}
	inline uint64 getIdleNodeEndOffset(_NODE_& node){
		return (node.offset + node.size);
	}
	inline void addIdleNodeAtBegin(uint64 offset, uint64 size){
		while(size > BLOCK_MAX_IDLE_NUMBER){
			m_idles.insert(m_idles.begin(), _NODE_(offset, BLOCK_MAX_IDLE_NUMBER));
			size -= BLOCK_MAX_IDLE_NUMBER;
			offset += BLOCK_MAX_IDLE_NUMBER;
		};
		m_idles.insert(m_idles.begin(), _NODE_(offset, size));
	}
	inline void addIdleNodeAtEnd(uint64 offset, uint64 size){
		while(size > BLOCK_MAX_IDLE_NUMBER){
			m_idles.push_back(_NODE_(offset, BLOCK_MAX_IDLE_NUMBER));
			size -= BLOCK_MAX_IDLE_NUMBER;
			offset += BLOCK_MAX_IDLE_NUMBER;
		};
		m_idles.push_back(_NODE_(offset, size));
	}

protected:
	
};

NS_HIVE_END


#endif /* idle_hpp */



