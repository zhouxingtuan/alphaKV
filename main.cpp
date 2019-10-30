//
//  main.cpp
//  test
//
//  Created by AppleTree on 17/2/26.
//  Copyright © 2017年 AppleTree. All rights reserved.
//

#include "alphakv.hpp"
#include <chrono>
#include <iostream>
#include <thread>
#include <time.h>
USING_NS_HIVE;

inline int64 get_time_us(void)
{
	std::chrono::time_point<std::chrono::system_clock> p = std::chrono::system_clock::now();
	return (int64)std::chrono::duration_cast<std::chrono::microseconds>(p.time_since_epoch())
		.count();
}
inline int64 get_time_ms(void)
{
	std::chrono::time_point<std::chrono::system_clock> p = std::chrono::system_clock::now();
	return (int64)std::chrono::duration_cast<std::chrono::milliseconds>(p.time_since_epoch())
		.count();
}

int main(int argc, const char *argv[])
{
	// insert code here...
	std::cout << "Hello, World!\n";

	AlphaKV *pKey = new AlphaKV();
	int64 openT = get_time_us();
	bool result = pKey->openDB("mydb");
	int64 openE = get_time_us();
	fprintf(stderr, "open key cost time us = %lld\n", openE - openT);
	if (true == result) {
		CharVector value;
		int64 s = get_time_us();
		fprintf(stderr, "s=%lld\n", s);
		int64 getFailedCount = 0;
		int64 setFailedCount = 0;
		char key[32] = { 0 };
		std::vector<float> feature(256);
		for (int64 i = 100000; i >= 0; --i) {
			sprintf(key, "%08lld", i);
			result = pKey->set(key, 8, (char *)feature.data(), sizeof(float) * feature.size());
			if (false == result) {
				++setFailedCount;
			}
		}
		int64 e = get_time_us();
		fprintf(stderr, "Key e=%lld setFailedCount=%lld\n", e, setFailedCount);
		fprintf(stderr, "Key cost us = %lld\n", e - s);
		s = e;
		for (int64 i = 100000; i >= 0; --i) {
			sprintf(key, "%08lld", i);
			uint32 length;
			char *ptr = pKey->get(key, 8, &length);
			if (NULL == ptr) {
				++getFailedCount;
			}
		}
		e = get_time_us();
		fprintf(stderr, "Key e=%lld getFailedCount=%lld\n", e, getFailedCount);
		fprintf(stderr, "Key cost us = %lld\n", e - s);

		std::string mykey = "welcome";
		std::string myvalue = "hello world!";
		result = pKey->set(mykey.c_str(), (uint32)mykey.length(), myvalue.c_str(),
						   (uint32)myvalue.length());
		fprintf(stderr, "set result = %d\n", (int)result);
		uint32 myvaluelen;
		char *ptr = pKey->get(mykey.c_str(), (uint32)mykey.length(), &myvaluelen);
		if (NULL == ptr) {
			fprintf(stderr, "get mykey failed = %s\n", mykey.c_str());
		} else {
			fprintf(stderr, "get myvalue length=%d data=%s\n", myvaluelen, ptr);
		}
	}
	// std::chrono::milliseconds timespan(10 * 1000);  // or whatever
	// std::this_thread::sleep_for(timespan);

	delete pKey;

	return 0;
}
