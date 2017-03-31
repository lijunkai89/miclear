/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : ����c����
 *    SystemName  : �������ϵͳ
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-����C����
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/26         ����           �����         �����ĵ�
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

/*��̬ȫ�ֱ�������*/
static char static_step_name[128+1]    = { 0 };  /*���ղ�������*/
static char static_dayend_date[8+1]    = { 0 };  /*��������*/
static char static_logic_date[8+1]    = { 0 };  /*�����߼���*/
static int  static_sys_status          = 0  ;        /*ϵͳ����״̬ 1-�ռ� 2-��������*/

/******************************************************
 ��������:   put_dayend_stepname
 ��������:   �������ղ�������
 ����˵����  stepname[in]:���ղ��������
 ����ֵ��    
*******************************************************/
void put_dayend_stepname( const char* stepname )
{
    strcpy( static_step_name, stepname );
}


/******************************************************
 ��������:   put_dayend_date
 ��������:   ������������
 ����˵����  dayend_date[in]:��������
 ����ֵ��    
*******************************************************/
void put_dayend_date( const char* dayend_date)
{
    strcpy( static_dayend_date, dayend_date );
}

/******************************************************
 ��������:   pub_logic_date
 ��������:   �����߼�����
 ����˵����  logic_date[in]:�߼�����
 ����ֵ��    
*******************************************************/
void pub_logic_date( const char* logic_date)
{
    strcpy( static_logic_date, logic_date );
}

/******************************************************
 ��������:   put_sys_status
 ��������:   ��������״̬
 ����˵����  sys_status[in]:����״̬
 ����ֵ��    
*******************************************************/
void put_sys_status( const int sys_status)
{
    static_sys_status = sys_status ;
}

/******************************************************
 ��������:   get_dayend_stepname
 ��������:   ��ȡ���ղ�������
 ����˵���� 
 ����ֵ��    ��ȡ���ղ�������
*******************************************************/
void get_dayend_stepname( char *out_dayend_stepname )
{
    strcpy( out_dayend_stepname , static_step_name );
}


/******************************************************
 ��������:   get_dayend_date
 ��������:   ��ȡ��������
 ����˵���� 
 ����ֵ��    ��ȡ��������
*******************************************************/
void get_dayend_date( char *out_dayend_date )
{
    strcpy( out_dayend_date , static_dayend_date );
}

/******************************************************
 ��������:   get_logic_date
 ��������:   ��ȡ�����߼�������
 ����˵���� 
 ����ֵ��    ��ȡ�����߼�������
*******************************************************/
void get_logic_date( char *out_logic_date )
{
    strcpy( out_logic_date , static_logic_date );
}

/******************************************************
 ��������:   get_sys_status
 ��������:   ��ȡϵͳ״̬
 ����˵���� 
 ����ֵ��    ��ȡϵͳ״̬
*******************************************************/
int get_sys_status( )
{
    return static_sys_status;
}

/******************************************************
 ��������:   TraceLog
 ��������:   ��¼��־��ָ�����ļ�
 ����˵����  
             ucLogType[in]: ��־�ļ������DEBUG,INFO,WARNING��ERROR
             szPrefix[in]:  �Զ�����ļ�ǰ׺
             szUdfType[in]: �Զ������־���ͣ�д�뵽��־�ļ���
             szFile[in]:    ��¼��־��Դ�����ļ���
             nLine[in]:     ��¼��־��Դ�����ļ����ڵ���
             szFormat[in]:  ��ʽ���ַ���
 ����ֵ��     �޷���ֵ
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
 *����ַ���
 *    str - ����ȡ�ַ���
 *    sfield - ��ȡ��
 *    delin - �ָ���
 *�˺���ֻ����ָ������Ҳ���ַ����������Ƿ�ָ�������
 **/
char *StrCut( char *str , char *sfield , char delin )
{
    char        *sRetBuf;    //��ȡ��ʣ�µ��ַ�������һ���ַ��Ƿָ�����
    
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
 *ʩ���ļ�������,��ֹ����
 *      file_name - ������׺��չ��
 **/
int lock_file( const char* file_name )
{
    FILE* g_fp_lock = NULL;
    char lock_filename[255+1] = { 0 }; //���ļ�������
    char lockdir[255+1];               //���ļ���Ŀ¼
    int  fd = -1;                      //���ļ�������
    
    //���δָ�����ļ��Ĵ��Ŀ¼,��Ĭ�ϴ�ŵ�${HOME}/etc/lockĿ¼֮��
    if( getenv( "LOCKDIR" ) )
    {
        strncpy( lockdir, (char *)getenv( "LOCKDIR" ), sizeof( lockdir ) );
    }
    else
    {
        snprintf( lockdir, sizeof(lockdir), "%s/lock", getenv( "HOME" ) );
    }
       
    //�����ļ�,��ִ������
    snprintf( lock_filename, sizeof( lock_filename ),
        "%s/%s.lck",
        lockdir , file_name
        );
    g_fp_lock = fopen( lock_filename, "w" );
    if( NULL == g_fp_lock )
    {
        SysLog( LOGTYPE_ERROR , "�����ļ�[%s]ʧ��[%s]" , 
                  lock_filename , strerror( errno ) );
        return ERROR;
    }
    
    //��ȡ�ļ�������,�������ļ�����
    fd = fileno( g_fp_lock );

    if( -1 == lockf( fd, F_TLOCK, 1 ) )
    {
        if( EACCES == errno || EAGAIN == errno )
        {
            SysLog( LOGTYPE_ERROR , "��������ִ��֮��,���ܲ���ִ��" );
        }
        else
        {
            SysLog( LOGTYPE_ERROR , "�������ļ�[%s]ʧ��[%s]" , 
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
 *���������Ϣ
 **/
static int read_master_cfg( char *sLineValue , char *sValue , char c)
{
    int i = 0 , len = 0;
        
    /*ȥ����β�Ľ�����*/
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
 *��ȡ�����ļ�
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
        SysLog(LOGTYPE_DEBUG, "�����ļ�[%s]�򿪴��� [%d]-[%s]", sCfgFileName , errno , strerror(errno) );
        return ERROR;
    }
        
    while( !feof(etc_fp) )
    {
        memset( buff , 0x00 , sizeof( buff ) );
        memset( bufftmp , 0x00 , sizeof( bufftmp ) );
        fgets(buff, 128, etc_fp);
                
        /*��ע����*/
        if ( buff[0] == cComment )
        {
            continue;
        }
        /*ȡvalue*/
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
