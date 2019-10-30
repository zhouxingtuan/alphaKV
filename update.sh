#!/bin/sh

process() {
	for file in `find $1 -maxdepth 1 -regextype posix-extended -regex ".*\.(hpp|cpp|h|c)"`
	do
		echo $file
		clang-format $file > a
		mv a $file
	done
}

process "."
