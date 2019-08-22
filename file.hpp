//
//  file.hpp
//  base
//
//  Created by AppleTree on 16/12/18.
//  Copyright © 2016年 AppleTree. All rights reserved.
//

#ifndef file_hpp
#define file_hpp

// 开启大文件
# define _LARGE_FILE       1
# ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
# endif
# define _LARGEFILE_SOURCE 1

// 统一所有数据类型的使用
#ifndef uint64
typedef char 						int8;
typedef short 						int16;
typedef int 						int32;
typedef long long int				int64;
typedef unsigned char 				uint8;
typedef unsigned short  			uint16;
typedef unsigned int    			uint32;
typedef unsigned long long int  	uint64;
#endif

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <sys/types.h>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>

// 在非苹果平台（linux）上面加载这个文件；使用open,read,write操作文件的读写
#ifndef __APPLE__

#ifdef __ANDROID__
#include <asm/io.h>
#define USE_STREAM_FILE
#else
#include <sys/io.h>
#endif

#include <fcntl.h>
#else
#define USE_STREAM_FILE
#endif

// 安卓平台上面需要显示的调用lseek的版本
#ifdef USE_STREAM_FILE
# define fseek fseeko
# define ftell ftello
#endif

#ifdef __ANDROID__
# define lseek lseek64
#endif

// 命名空间定义
#ifndef NS_HIVE_BEGIN
#define NS_HIVE_BEGIN namespace HiveNS {
#endif
#ifndef NS_HIVE_END
#define NS_HIVE_END };
#endif
#ifndef USING_NS_HIVE
#define USING_NS_HIVE using namespace HiveNS;
#endif

NS_HIVE_BEGIN

#define BASE_FILE_DESC "base 1.0 AppleTree@2016"        // 28个字节以内

enum FileError{
	FILE_OK = 0,
	FERR_TOUCH_FAILED,
	FERR_INVALID_FILE,
	FERR_OPENRW_FAILED,
	FERR_INIT_WRITE_FAILED,
	FERR_ARRAY_OUT_OF_RANGE,
	FERR_ARRAY_NOT_SET,
	FERR_ARRAY_SET_FAILED,
	FERR_UNIT_SIZE_NOT_MATCH,
	FERR_BLOCK_SIZE_NOT_MATCH,
	FERR_BLOCK_TOO_LARGE,
	FERR_BLOCK_READ_FAIL,
	FERR_BLOCK_SET_FAILED,
	FERR_BLOCK_EMPTY,
	FERR_KEY_IS_TOO_LONG,
	FERR_KEY_NOT_FOUND,
	FERR_KEY_SET_FAILED,
	FERR_KEY_VALUE_SIZE_NOT_MATCH,
	FERR_KEY_LENGTH_NOT_MATCH,
	FERR_KEY_ALREADY_EXIST,
};

#define BLOCK_SIZE 64					// 每个文件块的大小
#define EXPAND_BLOCK_SIZE 8192			// 文件扩展步长
#define MAX_EXPAND_BLOCK_SIZE 67108864	// 64M，最大保存的单个文件块长度

class File
{
public:
	std::string m_fileName;		// 文件名
	int64 m_fileLength;			// 文件长度
#ifdef USE_STREAM_FILE
	FILE* m_pFile;				// 文件句柄
#else
	int m_fileHandle;			// linux下文件句柄
#endif
public:
	File(const std::string& name, const std::string& ext) : m_fileName(name+ext), m_fileLength(0),
#ifdef USE_STREAM_FILE
	m_pFile(NULL)
#else
	m_fileHandle(0)
#endif
	{
		
	}
	virtual ~File(void){
		closeReadWrite();
	}
public:
	int touchFile(const char* checkHead, int checkLength){
		if(!openReadWrite("ab+")){
			return FERR_TOUCH_FAILED;
		}
		m_fileLength = writeTell();
		if(0 == m_fileLength){
			closeReadWrite();
			return FILE_OK;
		}
		if(NULL != checkHead){
			char head[checkLength];
			fileSeek(0, SEEK_SET);
			fileRead(head, 1, checkLength);
			if(strncmp(head, checkHead, checkLength) != 0){
				// 发生错误了，这里需要关闭文件
				closeReadWrite();
				fprintf(stderr, "touchFile check error\n");
				return FERR_INVALID_FILE;
			}
		}
		closeReadWrite();
		return FILE_OK;
	}
	bool openReadWrite(const char* mode){
		closeReadWrite();
#ifdef USE_STREAM_FILE
		m_pFile = fopen(m_fileName.c_str(), mode);
		if(NULL == m_pFile){
			fprintf(stderr, "openReadWrite open block failed file=%s\n", m_fileName.c_str());
			return false;
		}
#else
		if(*mode == 'a'){
			m_fileHandle = open(m_fileName.c_str(), O_RDWR|O_CREAT, 0);   // O_APPEND
		}else{
			m_fileHandle = open(m_fileName.c_str(), O_RDWR);
		}
		if(-1 == m_fileHandle){
			m_fileHandle = 0;
			fprintf(stderr, "openReadWrite open block failed file=%s\n", m_fileName.c_str());
			return false;
		}
#endif
//		fprintf(stderr, "openReadWrite ok file=%s\n", m_fileName.c_str());
		return true;
	}
	void closeReadWrite(void){
#ifdef USE_STREAM_FILE
		if(NULL != m_pFile){
			fclose(m_pFile);
			m_pFile = NULL;
		}
#else
		if(0 != m_fileHandle){
			close(m_fileHandle);
			m_fileHandle = 0;
		}
#endif
	}
	inline void setFileName(const char* fileName){
		m_fileName = fileName;
	}
	inline bool saveData(const void* ptr, int64 length, int64 offset, int64 expandSize, bool recordLength){
		int saveLength;
		if(recordLength){
			saveLength = 4 + (int)length;
		}else{
			saveLength = (int)length;
		}
		// 这个数据的保存不超出当前文件的长度
		int64 endOffset = offset + (int64)saveLength;
		if(endOffset <= m_fileLength){
			if(recordLength){
#ifdef USE_STREAM_FILE
				if(4 == seekWrite(&saveLength, 1, 4, offset, SEEK_SET)){
					if(length == fileWrite(ptr, 1, length)){
						flush();
						return true;
					}
				}
#else
				// Linux 下需要拼接数据块，之后再一次性写入
				char* saveBuffer = new char[saveLength];
				*((int*)saveBuffer) = saveLength;
				memcpy(saveBuffer + 4, ptr, length);
				if((int64)saveLength == seekWrite(saveBuffer, 1, saveLength, offset, SEEK_SET)){
					delete []saveBuffer;
					return true;
				}
				delete []saveBuffer;
#endif
				return false;
			}else{
				if(length == seekWrite(ptr, 1, length, offset, SEEK_SET)){
#ifdef USE_STREAM_FILE
					flush();
#endif
					return true;
				}
				return false;
			}
		}
		// 这个数据写入的偏移在文件的内部，特殊处理文件增长的数值
		if(offset <= m_fileLength){
			fileSeek(offset, SEEK_SET);
#ifdef USE_STREAM_FILE
			// 数据是跨数据块的，直接写入数据，后面再对齐文件
			if(recordLength){
				if(4 != fileWrite(&saveLength, 1, 4)){
					flush();
					return false;
				}
			}
			if(length != fileWrite(ptr, 1, length)){
				flush();
				return false;
			}
			m_fileLength = endOffset;	// 写入后文件扩展长度
			// 检查是否需要补齐数据
			if(0 != expandSize){
				int64 alignLength = m_fileLength % expandSize;
				if(alignLength != 0){
					alignLength = expandSize - alignLength;
					char tempBuffer[expandSize];
					memset(tempBuffer, 0, alignLength);
					if(alignLength != fileWrite(tempBuffer, 1, alignLength)){
						flush();
						return false;
					}
					m_fileLength += alignLength;
				}
			}
			flush();
#else
			// Linux 下需要拼接数据块，之后再一次性写入
			int64 alignLength = 0;
			if(0 != expandSize){
				alignLength = (endOffset) % expandSize;
				if(alignLength != 0){
					alignLength = expandSize - alignLength;
				}
			}
			int64 saveBufferSize = saveLength + alignLength;
			char* saveBuffer = new char[saveBufferSize];
			memset(saveBuffer, 0, saveBufferSize);
			if(recordLength){
				*((int*)saveBuffer) = saveLength;
				memcpy(saveBuffer + 4, ptr, length);
			}else{
				memcpy(saveBuffer, ptr, length);
			}
			if(saveBufferSize != fileWrite(saveBuffer, 1, saveBufferSize)){
				delete []saveBuffer;
				return false;
			}
			m_fileLength = offset + saveBufferSize;
			delete []saveBuffer;
#endif
			return true;
		} // end offset < m_fileLength
		// 数据写入的偏移在文件在文件的长度以外，需要填充部分数据
		fileSeek(0, SEEK_END);
		// (1) 检查总长度，在可接受的范围内，直接生成并拼接写入
		int64 alignLength = 0;
		if(0 != expandSize){
			alignLength = (endOffset) % expandSize;
			if(alignLength != 0){
				alignLength = expandSize - alignLength;
			}
		}
		int64 blankSize = offset - m_fileLength;
		int64 totalWriteSize = blankSize + saveLength + alignLength;
		if(totalWriteSize <= MAX_EXPAND_BLOCK_SIZE){
			char* saveBuffer = new char[totalWriteSize];
			memset(saveBuffer, 0, totalWriteSize);
			if(recordLength){
				*((int*)(saveBuffer + blankSize)) = saveLength;
				memcpy(saveBuffer + blankSize + 4, ptr, length);
			}else{
				memcpy(saveBuffer + blankSize, ptr, length);
			}
			if(totalWriteSize != fileWrite(saveBuffer, 1, totalWriteSize)){
				delete []saveBuffer;
				return false;
			}
			m_fileLength += totalWriteSize;
			delete []saveBuffer;
			return true;
		}
		// (2) 区分前半部分（空白填充）和后半部分（真实数据）写入
		char* saveBuffer = new char[MAX_EXPAND_BLOCK_SIZE];
		memset(saveBuffer, 0, MAX_EXPAND_BLOCK_SIZE);
		// 写入空白数据
		while (blankSize > MAX_EXPAND_BLOCK_SIZE) {
			if(MAX_EXPAND_BLOCK_SIZE != fileWrite(saveBuffer, 1, MAX_EXPAND_BLOCK_SIZE)){
				delete []saveBuffer;
				return false;
			}
			blankSize -= MAX_EXPAND_BLOCK_SIZE;
			m_fileLength += MAX_EXPAND_BLOCK_SIZE;
		}
		if(blankSize != fileWrite(saveBuffer, 1, blankSize)){
			delete []saveBuffer;
			return false;
		}
		m_fileLength += blankSize;
		// 写入后面的数据
		int64 saveBufferSize = saveLength + alignLength;
		if(recordLength){
			*((int*)(saveBuffer)) = saveLength;
			memcpy(saveBuffer + 4, ptr, length);
		}else{
			memcpy(saveBuffer, ptr, length);
		}
		if(saveBufferSize != fileWrite(saveBuffer, 1, saveBufferSize)){
			delete []saveBuffer;
			return false;
		}
		m_fileLength += saveBufferSize;
		delete []saveBuffer;
		return true;
	}
	inline int64 seekRead(void * ptr, int64 size, int64 n, int64 offset, int seek){
		fileSeek(offset, seek);
		return fileRead(ptr, size, n);
	}
	inline int64 seekWrite(const void * ptr, int64 size, int64 n, int64 offset, int seek){
		fileSeek(offset, seek);
		return fileWrite(ptr, size, n);
	}
#ifdef USE_STREAM_FILE
	// 文件读写操作
	inline int64 fileRead(void * ptr, int64 size, int64 n){
		return fread(ptr, size, n, m_pFile);
	}
	inline int64 fileWrite(const void * ptr, int64 size, int64 n){
		return fwrite(ptr, size, n, m_pFile);
	}
	inline int64 fileSeek(int64 offset, int seek){
		return fseek(m_pFile, offset, seek);
	}
	inline int64 readTell(void){
		return ftell(m_pFile);
	}
	inline int64 writeTell(void){
		return ftell(m_pFile);
	}
	inline int flush(void){
		return fflush(m_pFile);
	}
	// data 文件读写操作
	inline bool isOpen(void){
		return (NULL != m_pFile);
	}
#else
	// block 文件读写操作
	inline int64 fileRead(void * ptr, int64 size, int64 n){
		return read(m_fileHandle, ptr, size*n);
	}
	inline int64 fileWrite(const void * ptr, int64 size, int64 n){
		return write(m_fileHandle, ptr, size*n);
	}
	inline int64 fileSeek(int64 offset, int seek){
		return lseek(m_fileHandle, offset, seek);
	}
	inline int64 readTell(void){
		return fileSeek(0, SEEK_END);
	}
	inline int64 writeTell(void){
		return fileSeek(0, SEEK_END);
	}
	inline int flush(void){
		return 0;
	}
	inline bool isOpen(void){
		return (0 != m_fileHandle);
	}
#endif
};



NS_HIVE_END

#endif /* file_hpp */







