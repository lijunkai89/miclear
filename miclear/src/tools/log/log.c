/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : ��־����
 *    SystemName  : �������ϵͳ
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : ���ݷ���ϵͳ-����C����
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/26         ����           �����         �����ĵ�
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


/*�����־ǰ׺������*/
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


/*Ĭ��ǰ׺�̻߳�����*/
static pthread_mutex_t g_default_prefix_mutex = PTHREAD_MUTEX_INITIALIZER;
/*��������ȥ�б�Ļ�����*/
static pthread_mutex_t g_prefix_list_mutex    = PTHREAD_MUTEX_INITIALIZER;

/*��ǰ�������б��е�����*/
static int g_nCurrMutexNum = 0;
/*���ÿ��ǰ׺�Ļ������б�*/ 
static LogPrefixMutexInfo g_logPrefixMutexs[MAX_LOG_PREFIX_NUM] = {
    {"", NULL}
}; 

/**
 *���Ҵ�����־��������Ϣ
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
 *�Ȳ����Ƿ���ڴ�ǰ׺�Ļ�����
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
 *֮ǰû�г��ֹ���ǰ׺,Ϊ�䴴��һ��������
 **/
static pthread_mutex_t*  __CreateLogPrefixMutex( const char* szPrefix )
{
    pthread_mutex_t* mutex = NULL;
    
    if( g_nCurrMutexNum >= MAX_LOG_PREFIX_NUM )
    {
        return NULL; /*����������޶�*/
    }    
    
    mutex = (pthread_mutex_t*)malloc( sizeof( pthread_mutex_t ) );
    if( NULL == mutex )
    {
        return NULL; /*���ش���,�Ѻľ�??*/
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
 *������־ǰ׺ʩ���̻߳�����
 **/
static pthread_mutex_t*  __LogFileThreadLock( const char* szPrefix )
{
    pthread_mutex_t* mutex = NULL;

    /*�ж��Ƿ�ʹ��Ĭ�ϵ��̻߳�����*/
    if( NULL == szPrefix || 0 == szPrefix[ 0 ] || 
        strlen( szPrefix ) >= sizeof( g_logPrefixMutexs[ 0 ].szPrefix ) )
    {
        mutex = &g_default_prefix_mutex;
    }
    
    if( NULL == mutex )
    {
        /*�������Ĭ��ǰ׺,�򴴽�������,����������֮ǰ,��Ҫ�Ա����������б�Ļ���������*/
        if( 0 != pthread_mutex_lock( &g_prefix_list_mutex ) )
        {
            return NULL;
        }       

        /*�Ȳ����Ƿ���ڴ�ǰ׺�Ļ�����*/
        mutex = __FindPrefixMutex( szPrefix );
        if( NULL == mutex )
        {
            //֮ǰû�г��ֹ���ǰ׺,Ϊ�䴴��һ��������
            mutex = __CreateLogPrefixMutex( szPrefix );
        }

        /*�ͷŻ������ı�����*/
        pthread_mutex_unlock( &g_prefix_list_mutex );                
    }
    
    if( NULL == mutex )
    {
        mutex = &g_default_prefix_mutex;
    }
    
    
    /*�Ըû���ȥ��������*/
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
    
    /*����־�ļ�*/
    fd = open( szFileName, 
        O_RDWR | O_APPEND | O_CREAT,
        S_IRUSR | S_IWUSR | S_IRGRP );
    if( -1 == fd )
    {
        fprintf( stderr, "%s\n", strerror(errno) );
        return NULL; /*���ļ�ʧ��*/
    }
    
    /*���ļ�ʩ��д�ȴ���*/
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
                continue; /*���ź��ж�,����ʩ��д�ȴ���*/
            }
        }
        else
        {
            break;       /*ʩ��д�ȴ����ɹ�,����ѭ��*/
        }
    }
    
    /*ʹ�ñ�׼C������ʽ��*/
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
    
    /*������־�ļ���*/
    strftime( szTimeStr, sizeof( szTimeStr ), "%Y%m%d", &tm );   
    
    /*��ȡ��־�ļ�Ŀ¼*/
    if( getenv( "LOGDIR" ) )
    {
        strcpy( szLogDir, getenv( "LOGDIR" ) );   /*�ӻ��������ж�ȡ*/
    }
    else
    {
        /*���δ���û�����������Ĭ��Ϊ$HOME/log*/
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
    
    /*���ļ�,�����ļ�ʩ��д�ȴ���*/
    if( NULL == ( fp = __OpenLockFile( szLogFileName ) ) )
    {
        return; /*����־�ļ�ʧ��*/
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
        return ; //������־�����¼
    }
    
    /*���Ҵ�����־��������Ϣ*/
    if( NULL == ( pLogTypeInfo = __FindLogTypeInfo( ucLogType ) ) )
    {
        return ; /*�����ڵ���־����*/
    }

    /*������־ǰ׺ʩ���̻߳�����*/
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
					 
		/*�ͷ��̻߳�����*/
		pthread_mutex_unlock( mutex );
}
