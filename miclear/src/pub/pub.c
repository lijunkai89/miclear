/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 公共c函数
 *    SystemName  : 清分清算系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-公共C函数
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/26         北京           李君凯         创建文档
******************************************************************************/

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>

#include "sys_const_def.h"
#include "app_const_def.h"
#include "appdef.h"

extern char sExecName[];

/*静态全局变量定义*/
static char static_step_name[128+1]    = { 0 };  /*日终步骤名称*/
static char static_dayend_date[8+1]    = { 0 };  /*日终日期*/
static char static_logic_date[8+1]    = { 0 };  /*联机逻辑日*/
static int  static_sys_status          = 0  ;        /*系统日期状态 1-日间 2-正在日终*/

/******************************************************
 函数名称:   put_dayend_stepname
 函数功能:   设置日终步骤名称
 参数说明：  stepname[in]:日终步骤的名称
 返回值：    
*******************************************************/
void put_dayend_stepname( const char* stepname )
{
    strcpy( static_step_name, stepname );
}


/******************************************************
 函数名称:   put_dayend_date
 函数功能:   设置日终日期
 参数说明：  dayend_date[in]:日终日期
 返回值：    
*******************************************************/
void put_dayend_date( const char* dayend_date)
{
    strcpy( static_dayend_date, dayend_date );
}

/******************************************************
 函数名称:   pub_logic_date
 函数功能:   设置逻辑日期
 参数说明：  logic_date[in]:逻辑日期
 返回值：    
*******************************************************/
void pub_logic_date( const char* logic_date)
{
    strcpy( static_logic_date, logic_date );
}

/******************************************************
 函数名称:   put_sys_status
 函数功能:   设置日终状态
 参数说明：  sys_status[in]:日终状态
 返回值：    
*******************************************************/
void put_sys_status( const int sys_status)
{
    static_sys_status = sys_status ;
}

/******************************************************
 函数名称:   get_dayend_stepname
 函数功能:   获取日终步骤名称
 参数说明： 
 返回值：    获取日终步骤名称
*******************************************************/
void get_dayend_stepname( char *out_dayend_stepname )
{
    strcpy( out_dayend_stepname , static_step_name );
}


/******************************************************
 函数名称:   get_dayend_date
 函数功能:   获取日终日期
 参数说明： 
 返回值：    获取日终日期
*******************************************************/
void get_dayend_date( char *out_dayend_date )
{
    strcpy( out_dayend_date , static_dayend_date );
}

/******************************************************
 函数名称:   get_logic_date
 函数功能:   获取联机逻辑日日期
 参数说明： 
 返回值：    获取联机逻辑日日期
*******************************************************/
void get_logic_date( char *out_logic_date )
{
    strcpy( out_logic_date , static_logic_date );
}

/******************************************************
 函数名称:   get_sys_status
 函数功能:   获取系统状态
 参数说明： 
 返回值：    获取系统状态
*******************************************************/
int get_sys_status( )
{
    return static_sys_status;
}

/******************************************************
 函数名称:   TraceLog
 函数功能:   记录日志到指定的文件
 参数说明：  
             ucLogType[in]: 日志文件的类别，DEBUG,INFO,WARNING或ERROR
             szPrefix[in]:  自定义的文件前缀
             szUdfType[in]: 自定义的日志类型，写入到日志文件中
             szFile[in]:    记录日志的源代码文件名
             nLine[in]:     记录日志的源代码文件所在的行
             szFormat[in]:  格式化字符串
 返回值：     无返回值
*******************************************************/
void sys_log( unsigned char ucLogType , 
            const char* szFile , 
            int nLine ,
            const char* szFormat , ... )
{
    char log_step_name[128+1]    = { 0 };
    va_list ap ;
    va_start( ap , szFormat ) ;
    
    get_dayend_stepname( log_step_name );
    
    vlog( ucLogType , 
          log_step_name , 
          szFile , 
          nLine ,
          szFormat , 
          ap );
          
    
    va_end( ap );
    
}

/**
 *拆分字符串
 *    str - 待截取字符串
 *    sfield - 截取域
 *    delin - 分隔符
 *此函数只处理分隔符在右侧的字符串，不论是否分隔符结束
 **/
char *StrCut( char *str , char *sfield , char delin )
{
    char        *sRetBuf;    //截取后剩下的字符串（第一个字符非分隔符）
    
    sRetBuf = strchr( str, delin );
    
    if( sRetBuf != NULL )
    {
        if( ( sRetBuf - str ) > 0 )
        {
            memcpy( sfield , str , sRetBuf - str );
        }
        return sRetBuf + 1;
    }
    else
    {
        memcpy( sfield , str , strlen( str ) );
        return NULL;
    }
}




/**
 *施加文件互斥锁,防止并发
 *      file_name - 不带后缀扩展名
 **/
int lock_file( const char* file_name )
{
    FILE* g_fp_lock = NULL;
    char lock_filename[255+1] = { 0 }; //锁文件的名称
    char lockdir[255+1];               //锁文件的目录
    int  fd = -1;                      //锁文件描述符
    
    //如果未指定锁文件的存放目录,则默认存放到${HOME}/etc/lock目录之中
    if( getenv( "LOCKDIR" ) )
    {
        strncpy( lockdir, (char *)getenv( "LOCKDIR" ), sizeof( lockdir ) );
    }
    else
    {
        snprintf( lockdir, sizeof(lockdir), "%s/lock", getenv( "HOME" ) );
    }
       
    //打开锁文件,并执行锁定
    snprintf( lock_filename, sizeof( lock_filename ),
        "%s/%s.lck",
        lockdir , file_name
        );
    g_fp_lock = fopen( lock_filename, "w" );
    if( NULL == g_fp_lock )
    {
        SysLog( LOGTYPE_ERROR , "打开锁文件[%s]失败[%s]" , 
                  lock_filename , strerror( errno ) );
        return ERROR;
    }
    
    //获取文件描述符,并进行文件锁定
    fd = fileno( g_fp_lock );

    if( -1 == lockf( fd, F_TLOCK, 1 ) )
    {
        if( EACCES == errno || EAGAIN == errno )
        {
            SysLog( LOGTYPE_ERROR , "任务正在执行之中,不能并行执行" );
        }
        else
        {
            SysLog( LOGTYPE_ERROR , "锁定锁文件[%s]失败[%s]" , 
                      lock_filename , strerror( errno ) );
        }
        return ERROR;
    }
    
    return NOERR;
}

/*
* Delete the invalid space or tab at the string's tail.
*/
void DelSpace(char *string)
{
     int i;
 
     if (string == NULL)
         return;
     for (i = strlen(string)-1; i >= 0; i --) {
         if (string[i] != ' ' && string[i] != '\t') break;
     }
     string[i+1] = '\0';
     return;
}



/**
 *拆分配置信息
 **/
static int read_master_cfg( char *sLineValue , char *sValue , char c)
{
    int i = 0 , len = 0;
        
    /*去掉行尾的结束符*/
    len = strlen( sLineValue );
    for ( i = 0 ; i < len ; i++ )
    {
        if ( sLineValue[i] == '\n' )
            sLineValue[i] = '\0';
        else if ( sLineValue[i] == 0x0D )
            sLineValue[i] = '\0';
        else
            continue;
    }
    
    for ( i = 0 ; i < len ; i++ )
    {
        if ( sLineValue[i] == c )
        {
            strcpy( sValue , &sLineValue[i+1] );
            break;
        }
    }
    
    return 0;
}




#if 0
/**
 *读取配置文件
 **/
int get_etc_file( char *key , char *value  )
{
    FILE        *etc_fp = NULL;
    char        sCfgFileName[60] = { 0 };
    char        buff[120] = { 0 };
    char        bufftmp[120] = { 0 };
    const char  cComment = '#';  
    const char  cdelim = '=';  
   
    sprintf( sCfgFileName , "%s/%s/%s" , getenv( "HOME" ) , DAYEND_CFG_DIR , DAYEND_CFG_NAME );
        
    if ( (etc_fp = fopen ( sCfgFileName , "r" ) ) == NULL)
    {
        SysLog(LOGTYPE_DEBUG, "配置文件[%s]打开错误 [%d]-[%s]", sCfgFileName , errno , strerror(errno) );
        return ERROR;
    }
        
    while( !feof(etc_fp) )
    {
        memset( buff , 0x00 , sizeof( buff ) );
        memset( bufftmp , 0x00 , sizeof( bufftmp ) );
        fgets(buff, 128, etc_fp);
                
        /*备注跳过*/
        if ( buff[0] == cComment )
        {
            continue;
        }
        /*取value*/
        else if ( strncmp( buff , key , strlen( key ) ) == 0 )
        {
            read_master_cfg( buff , bufftmp , cdelim );
            sprintf( value , "%s/%s" , getenv("HOME") , bufftmp );
            SysLog(LOGTYPE_DEBUG, "key[%s]value[%s]", key, bufftmp);
            continue;
        }        
        else
        {
            continue;
        }
    }

    fclose(etc_fp);
        
    return NOERR;
}
#endif
