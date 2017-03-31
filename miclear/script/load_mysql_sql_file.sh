#!/bin/sh
mysql -h $MYSQL_HOST -u$1 -p$2 << EOF
use $MYSQL_NAME; 
source $3;
exit
EOF
