#!/bin/sh

#当日文件目录
localdir_day=$LIQUIDATE_FILE_LOCAL/$1/$2
remotedir_day=./$1/$2

#创建本地机构当日文件目录
if [ -d $localdir_day ]
then
echo "exist"
else
mkdir -p $localdir_day
fi

#切换工作目录
cd $localdir_day
rm -f *
#登录远程服务器
user=upay
psw=payu123
addr=10.105.200.211
port=2222

lftp -u $user,$psw sftp://$addr:$port <<!

cd $remotedir_day

mget *

close
bye
!

#解压文件到当前目录
#uncompress $2

