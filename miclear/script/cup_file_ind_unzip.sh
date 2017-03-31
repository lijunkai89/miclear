#!/bin/sh

#当日文件目录
localdir_day=$LIQUIDATE_FILE_LOCAL/$1

#切换工作目录
cd $localdir_day

#解压文件到当前目录
#uncompress $2
gunzip $2
