/****************************************************************************
 *    Copyright (c) :裕福集团-软件公司.                      *
 *    ProgramName : main
 *    SystemName  : 裕福支付数据分析系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : Linux Server release 6.3
                    Oracle  11g
 *    Description : 数据分析系统-公共功能函数头文件
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/23         北京           李君凯         创建文档
****************************************************************************/

#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
//#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/un.h>

#include "mysql.h"
#include "log.h"

#define SAFE_FDCLOSE( fd ) \
do{\
    if( fd >= 0 ) \
    { \
        while( -1 == close( fd ) && EINTR == errno ); \
        fd = -1; \
    } \
}while(0)

#define SAFE_FCLOSE( fp ) \
do{ \
    if( fp ) \
    { \
        fclose( fp ); \
        fp = NULL; \
    } \
}while(0)

typedef int ( * step_func_api ) (MYSQL *conn_ptr); 

#define SysLog( logtype , fmt , arg... ) \
do{ \
    if ( logtype >= atoi( getenv( "LOGLEVEL" ) ) ) \
    { \
        sys_log( logtype , __FILE__ , __LINE__ , fmt , ##arg ); \
    } \
}while(0)
#if 0
#define DayEndLog( logtype , fmt , arg... ) \
do{ \
		if ( logtype >= atoi( getenv( "LOGLEVEL" ) ) ) \
		{ \
				sys_log( logtype,  __FILE__ , __LINE__ , fmt , ##arg ); \
		} \
}while(0)
#endif
#define CHECK( RT , err_desc ) \
do{ \
    if( ( NOERR != RT ) ) \
    { \
    		SysLog( LOGTYPE_ERROR, "执行[%s]失败 %s", #RT , err_desc ); \
    		return ERROR; \
    } \
}while(0)
