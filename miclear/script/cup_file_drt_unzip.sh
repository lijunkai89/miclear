#!/bin/sh

#本地机构号当日文件目录
localdir_day=$LIQUIDATE_FILE_LOCAL/$1/$2
echo $localdir_day

#切换工作目录
cd $localdir_day

#解压文件到当前目录
echo $3
#uncompress $3
gunzip $3

file_tar=${3%.*}
echo $file_tar
tar -zxvf $file_tar

#解压商户资金划付文件
#uncompress $4
gunzip $4
