#!/bin/sh

#�����ļ�Ŀ¼
localdir_day=$LIQUIDATE_FILE_LOCAL/$1/$2
remotedir_day=./$1/$2

#�������ػ��������ļ�Ŀ¼
if [ -d $localdir_day ]
then
echo "exist"
else
mkdir -p $localdir_day
fi

#�л�����Ŀ¼
cd $localdir_day
rm -f *
#��¼Զ�̷�����
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

#��ѹ�ļ�����ǰĿ¼
#uncompress $2

