#!/bin/sh

src=$1
dst=$2

hdfs dfs -test -e $dst

if [ $? -eq 1 ]; then
    path=`dirname $dst`
    hdfs dfs -mkdir -p $path
    hdfs dfs -put $src $dst
fi
