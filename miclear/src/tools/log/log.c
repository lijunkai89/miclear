/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 日志函数
 *    SystemName  : 清分清算系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 数据分析系统-公共C函数
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/26         北京           李君凯         创建文档
******************************************************************************/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sys/types.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include "log.h"


/*最大日志前缀的数量*/
#define MAX_LOG_PREFIX_NUM 40  

static unsigned char g_ucLogFilter = 0xFF;

typedef struct LogTypeInfo
{
	unsigned char ucLogType;
	char          szName[100];
}LogTypeInfo;

typedef struct LogPrefixMutexInfo
{
    char    szPrefix[30+1];
    pthread_mutex_t* mutex;
}LogPrefixMutexInfo;

static const LogTypeInfo g_LogRegister[] = {
	{ LOGTYPE_ERROR,   "ERROR  "     },
	{ LOGTYPE_WARNING, "WARNING"     },
	{ LOGTYPE_INFO,    "INFO   "     },
	{ LOGTYPE_DEBUG,   "DEBUG  "     },
	{ 0x00,            ""            }
};


/*默认前缀线程互斥区*/
static pthread_mutex_t g_default_prefix_mutex = PTHREAD_MUTEX_INITIALIZER;
/*保护互斥去列表的互斥区*/
static pthread_mutex_t g_prefix_list_mutex    = PTHREAD_MUTEX_INITIALIZER;

/*当前互斥区列表中的数量*/
static int g_nCurrMutexNum = 0;
/*针对每种前缀的互斥区列表*/ 
static LogPrefixMutexInfo g_logPrefixMutexs[MAX_LOG_PREFIX_NUM] = {
    {"", NULL}
}; 

/**
 *查找此类日志的配置信息
 **/
static const LogTypeInfo* __FindLogTypeInfo( unsigned char ucLogType )
{
    int i = 0;
    
    for( i = 0; g_LogRegister[i].ucLogType; i++ )
    {
        if( g_LogRegister[i].ucLogType == ucLogType )
        {
            return g_LogRegister + i;
        }
    }
    return NULL;
}


/**
 *先查找是否存在此前缀的互斥锁
 **/
static pthread_mutex_t*  __FindPrefixMutex( const char* szPrefix )
{
    int i = 0;
    pthread_mutex_t *mutex = NULL;
    
    for( i = 0; i < g_nCurrMutexNum; i++ )
    {
        if( 0 == strcmp( g_logPrefixMutexs[i].szPrefix, szPrefix ) )
        {
            mutex = g_logPrefixMutexs[i].mutex;            
        }
    }
    
    return mutex;    
}


/**
 *之前没有出现过此前缀,为其创建一个互斥区
 **/
static pthread_mutex_t*  __CreateLogPrefixMutex( const char* szPrefix )
{
    pthread_mutex_t* mutex = NULL;
    
    if( g_nCurrMutexNum >= MAX_LOG_PREFIX_NUM )
    {
        return NULL; /*超过了最大限度*/
    }    
    
    mutex = (pthread_mutex_t*)malloc( sizeof( pthread_mutex_t ) );
    if( NULL == mutex )
    {
        return NULL; /*严重错误,堆耗尽??*/
    }
    
    if( 0 != pthread_mutex_init( mutex, NULL ) )
    {
        free( mutex );
        return NULL; 
    }
    
    g_logPrefixMutexs[g_nCurrMutexNum].mutex = mutex;
    strcpy(g_logPrefixMutexs[g_nCurrMutexNum].szPrefix,
        szPrefix );    
    g_nCurrMutexNum++;
        
    return mutex;    
}


/**
 *根据日志前缀施加线程互斥锁
 **/
static pthread_mutex_t*  __LogFileThreadLock( const char* szPrefix )
{
    pthread_mutex_t* mutex = NULL;

    /*判断是否使用默认的线程互斥锁*/
    if( NULL == szPrefix || 0 == szPrefix[ 0 ] || 
        strlen( szPrefix ) >= sizeof( g_logPrefixMutexs[ 0 ].szPrefix ) )
    {
        mutex = &g_default_prefix_mutex;
    }
    
    if( NULL == mutex )
    {
        /*如果不是默认前缀,则创建互斥区,创建互斥区之前,需要对保护互斥区列表的互斥区锁定*/
        if( 0 != pthread_mutex_lock( &g_prefix_list_mutex ) )
        {
            return NULL;
        }       

        /*先查找是否存在此前缀的互斥锁*/
        mutex = __FindPrefixMutex( szPrefix );
        if( NULL == mutex )
        {
            //之前没有出现过此前缀,为其创建一个互斥区
            mutex = __CreateLogPrefixMutex( szPrefix );
        }

        /*释放互斥区的保护锁*/
        pthread_mutex_unlock( &g_prefix_list_mutex );                
    }
    
    if( NULL == mutex )
    {
        mutex = &g_default_prefix_mutex;
    }
    
    
    /*对该互斥去进行锁定*/
    if( 0 != pthread_mutex_lock( mutex ) )
    {
        return NULL;
    }
    
    return mutex;
}



static FILE* __OpenLockFile( const char* szFileName )
{
    FILE* fp = NULL;
    int   fd = -1;
    
    /*打开日志文件*/
    fd = open( szFileName, 
        O_RDWR | O_APPEND | O_CREAT,
        S_IRUSR | S_IWUSR | S_IRGRP );
    if( -1 == fd )
    {
        fprintf( stderr, "%s\n", strerror(errno) );
        return NULL; /*打开文件失败*/
    }
    
    /*对文件施加写等待锁*/
    struct flock lock;
    lock.l_type   = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start  = 0;
    lock.l_len    = 1;
    
    while( 1 )
    {
        if( -1 == fcntl( fd, F_SETLKW, &lock ) )
        {
            if( EINTR != errno )
            {
                close( fd );
                fprintf( stderr, "%s\n", strerror(errno) );
                return NULL;
            }
            else
            {
                continue; /*被信号中断,继续施加写等待锁*/
            }
        }
        else
        {
            break;       /*施加写等待锁成功,跳出循环*/
        }
    }
    
    /*使用标准C的流方式打开*/
    fp = fdopen( fd, "a" );
    if( NULL == fp )
    {
        close( fd );
        return NULL;
    }
    
    return fp;
}



static void __log(const LogTypeInfo* pLogTypeInfo, 
		const char* szPrefix, 
    const char* szFile, 
    int nLine, 
    const char* szFormat, 
    va_list ap )
{
    struct tm tm;
    FILE* fp = NULL;
    char  szTimeStr[20+1] = { 0 };
    time_t now;
    char  szLogFileName[255+1] = { 0 };
    char  szLogDir[255+1] = { 0 };
    
    now = time( NULL );    
    localtime_r(&now, &tm );    
    
    /*构造日志文件名*/
    strftime( szTimeStr, sizeof( szTimeStr ), "%Y%m%d", &tm );   
    
    /*获取日志文件目录*/
    if( getenv( "LOGDIR" ) )
    {
        strcpy( szLogDir, getenv( "LOGDIR" ) );   /*从环境变量中读取*/
    }
    else
    {
        /*如果未设置环境变量，则默认为$HOME/log*/
        snprintf( szLogDir, sizeof( szLogDir ),
            "%s/log",
            getenv("HOME") );
    } 
    
    snprintf( szLogFileName, 
        sizeof( szLogFileName ),
        "%s/%s%s%s.log",
        szLogDir,
        szTimeStr,
        ( NULL != szPrefix && szPrefix[0] ) ? "_" : "" ,
        ( NULL != szPrefix ) ? szPrefix : "" 
         );
    
    /*打开文件,并对文件施加写等待锁*/
    if( NULL == ( fp = __OpenLockFile( szLogFileName ) ) )
    {
        return; /*打开日志文件失败*/
    }    
    
    strftime( szTimeStr, sizeof( szTimeStr ),
        "%Y-%m-%d %H:%M:%S",
        &tm );
    
    fprintf( fp, "[%s][%s][PID-%10d][%s:%d]\t",
        szTimeStr,
        pLogTypeInfo->szName,
        getpid( ),   
        szFile, 
        nLine );

    vfprintf(fp, szFormat, ap );

    fprintf( fp, "\n" );
    
    fclose( fp );
}


void vlog( unsigned char ucLogType , 
					 char* szPrefix , 
					 const char* szFile , 
					 int nLine ,
					 const char* szFormat , 
					 va_list ap )
{
		const LogTypeInfo* pLogTypeInfo = NULL; 
    pthread_mutex_t* mutex      = NULL;    

    if( ! ( g_ucLogFilter & ucLogType ) )
    {
        return ; //此类日志无需记录
    }
    
    /*查找此类日志的配置信息*/
    if( NULL == ( pLogTypeInfo = __FindLogTypeInfo( ucLogType ) ) )
    {
        return ; /*不存在的日志类型*/
    }

    /*根据日志前缀施加线程互斥锁*/
    if( NULL == ( mutex = __LogFileThreadLock( szPrefix ) ) )
    {
        return; 
    }

    __log( pLogTypeInfo, 
					 szPrefix , 
					 szFile , 
					 nLine ,
					 szFormat , 
					 ap );
					 
		/*释放线程互斥锁*/
		pthread_mutex_unlock( mutex );
}
