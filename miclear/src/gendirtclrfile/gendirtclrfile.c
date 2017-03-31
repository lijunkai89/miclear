/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������ϵͳ
 *    SystemName  : ���Ȳ���-��ȡֱ�������ļ�
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-��ȡֱ�������ļ�
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/02/28         ����           �����         �����ĵ�
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

#define sftp_drt_sh     "sftp_get_cup_file_drt.sh"
#define sftp_log      "sftp_get_cup_file.log"
#define sunzip_drt_sh     "cup_file_drt_unzip.sh"

static char  szDailyDate[9]    = { 0 };

/*�ļ���ȡ�����*/
#define RET_GET_SUCC  0
#define RET_GET_FAIL  -1

#define LOG_SQL_TITLE "��ȡ�����ļ�"

int func_ftp_init(MYSQL *conn_ptr)
{
    int iRet = 0;
    char  szSql[2048]    = { 0 };
    
    sprintf( szSql , 
            " update t_liquidate_file t "
            "    set t.f_load_time = '', "
            "        t.f_status = %d "
            "  where t.f_file_date = \'%s\' "
            "    and t.f_channel_no = %d "
            , LIQUIDATE_FILE_STATUS_INIT
            , szDailyDate
            , CHANNEL_CUP_DIRECT
    );
    
    iRet = db_update( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    SysLog( LOGTYPE_INFO , "��ʼ������ֱ�������ļ��������" ) ;  
        
    
    memset(szSql, 0x00, sizeof(szSql));
    sprintf( szSql , 
            " update t_liquidate_file t "
            "    set t.f_load_time = '', "
            "        t.f_status = %d "
            "  where t.f_file_date = \'%s\' "
            "    and t.f_channel_no = %d "
            , LIQUIDATE_FILE_STATUS_INIT
            , szDailyDate
            , CHANNEL_CUP_DAIFU
    );
    
    iRet = db_update( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    SysLog( LOGTYPE_INFO , "��ʼ���������˵�����" ) ;  
    
    return NOERR;
}


/**
 *�ļ��ѻ�ȡ����������״̬
 **/
static int gen_resp_file_end( MYSQL *conn_ptr )
{
    char  szSql[2048]    = { 0 };
    int    iRet          = 0;
    
    sprintf( szSql , 
            "  update t_liquidate_file t                " 
            "     set t.f_load_time = date_format(SYSDATE(),'%%Y-%%m-%%d %%H:%%i:%%s'), "
            "         t.f_status = %d "
            "   where t.f_file_date = \'%s\'  "
            "     and t.f_status = %d "
            "    and t.f_channel_no = %d "
            , LIQUIDATE_FILE_STATUS_GEN_SUCC 
            , szDailyDate 
            , LIQUIDATE_FILE_STATUS_INIT
            , CHANNEL_CUP_DIRECT
    );
    
    iRet = db_update( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet <= 0 )
    {
        return ERROR;
    }
    
    SysLog( LOGTYPE_INFO , "��ʼ������ֱ�������ļ��������" ) ;  
    
    memset(szSql, 0x00, sizeof(szSql));
    sprintf( szSql , 
            "  update t_liquidate_file t                " 
            "     set t.f_load_time = date_format(SYSDATE(),'%%Y-%%m-%%d %%H:%%i:%%s'), "
            "         t.f_status = %d "
            "   where t.f_file_date = \'%s\'  "
            "     and t.f_status = %d "
            "    and t.f_channel_no = %d "
            , LIQUIDATE_FILE_STATUS_GEN_SUCC 
            , szDailyDate 
            , LIQUIDATE_FILE_STATUS_INIT
            , CHANNEL_CUP_DAIFU
    );
    
    iRet = db_update( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet <= 0 )
    {
        return ERROR;
    }
    
    SysLog( LOGTYPE_INFO , "��ʼ���������˵�����" ) ;  
    
    return NOERR;
}

/**
 *get�ļ�
 **/
int source_commond(char *commond)
{
    int    status        = -1;
    
    status = system( commond );
    if ( -1 == status )
    {
        SysLog( LOGTYPE_ERROR , "system error" );
        return ERROR ;
    }
    else
    {
        if ( WIFEXITED( status ) )
        {
            if ( 0 == WEXITSTATUS ( status ) )
            {
                SysLog( LOGTYPE_INFO , "run shell script successfully" );
            }
            else
            {
                SysLog( LOGTYPE_ERROR , "run shell script fail, script exit code: %d", WEXITSTATUS( status ) );
                return ERROR ;
            }
        }
        else
        {
            SysLog( LOGTYPE_ERROR , "exit status = [%d]", WEXITSTATUS( status ) );
            return ERROR ;
        }
    }
    
    return NOERR;
}

/**
 *�ж��ļ��Ƿ��ȡ�ɹ�\
 *��ʱC�����ж��ļ��Ƿ�ftp�ɹ�
 *����ֵ�� NOERR-�ѻ�ȡ ERROR-δ��ȡ
 **/
int check_get_succ(char *acq_code, char *sFilePkNameAll, char *sFilePkNameAll1)
{
     FILE   *fp_chk_file  = NULL;
     char   commond[255]  = { 0 };
     
     memset( commond , 0x00 , sizeof( commond ) );
     sprintf( commond , "%s %s %s %s %s >> %s/%s_%s 2>&1" , 
               sunzip_drt_sh , acq_code, szDailyDate , sFilePkNameAll , sFilePkNameAll1, 
               getenv( "LOGDIR" ) , szDailyDate , sftp_log );
     SysLog( LOGTYPE_DEBUG , "%s" , commond );
         
     if ( (fp_chk_file = fopen( sFilePkNameAll , "rb" ) ) == NULL )
     {
         return ERROR ;
     }
     else if ( (fp_chk_file = fopen( sFilePkNameAll1 , "rb" ) ) == NULL )
     {
         return ERROR ;
     }
     else
     {
         //��ѹ�����ļ�
         system( commond );
         
         return NOERR;
     }
}
/**
 *ftp��ȡ���ռ����̻������ļ�
 **/
static int ftp_file_dirt(char acq_list_arr[][12])
{
    char   commond[255]  = { 0 };
    int    i, list_num;
    char   sFilePkName[255]  = { 0 };        /*Ŀ���ļ�����*/
    char   sFilePkNameAll[255]  = { 0 };     /*Ŀ���ļ�ȫ��*/
    char   sFilePkName1[255]  = { 0 };        /*Ŀ���ļ�����*/
    char   sFilePkNameAll1[255]  = { 0 };     /*Ŀ���ļ�ȫ��*/
    int    iRet = -1;

    /*��ȡ6λ���� YYMMDD*/
    
    for ( list_num = 0; list_num < MAX_ACQ_LIST; list_num++ )
    {
        memset( sFilePkName, 0x00, sizeof( sFilePkName ) );
        memset( sFilePkNameAll, 0x00, sizeof( sFilePkNameAll ) );
        memset( sFilePkName1, 0x00, sizeof( sFilePkName1 ) );
        memset( sFilePkNameAll1, 0x00, sizeof( sFilePkNameAll1 ) );
        
        SysLog( LOGTYPE_DEBUG , "��������[%s]" , acq_list_arr[list_num] );
        if ( strcmp( acq_list_arr[list_num] , "" ) == 0 )
            return NOERR;
        
        //�����ļ�ȫ����������·��
        sprintf( sFilePkName , "P_%s_%s.tar.gz.Z" , acq_list_arr[list_num], szDailyDate );
        sprintf( sFilePkName1 , "INO%s99ZPSUM.Z" , szDailyDate+2 );
        
        //�����ļ�ȫ����������·��
        sprintf( sFilePkNameAll , "%s/%s/%s/%s" , getenv( "LIQUIDATE_FILE_LOCAL" ) ,acq_list_arr[list_num], szDailyDate , sFilePkName );
        sprintf( sFilePkNameAll1 , "%s/%s/%s/%s" , getenv( "LIQUIDATE_FILE_LOCAL" ) ,acq_list_arr[list_num], szDailyDate , sFilePkName1 );
        SysLog( LOGTYPE_DEBUG , "ֱ���ļ���[%s]" , sFilePkNameAll );
        
        /*ƴ�ӵ���ѹ���ļ�����·��*/
        sprintf( commond , "%s %s %s >> %s/%s_%s 2>&1" , 
                 sftp_drt_sh , acq_list_arr[list_num], szDailyDate , 
                 getenv( "LOGDIR" ) , szDailyDate , sftp_log );
        SysLog( LOGTYPE_DEBUG , "%s" , commond );
        
        //�ȴ�ftp�ļ�
        for ( i = 1; i <= WAIT_MAX_TIMES ; i++ )
        {
            iRet = source_commond( commond );
            if ( NOERR != iRet )
            {
                return ERROR ;
            }
            
            iRet = check_get_succ( acq_list_arr[list_num], sFilePkNameAll, sFilePkNameAll1 );
            //iRet = RET_GET_SUCC;
            if ( RET_GET_SUCC != iRet )
            {
                SysLog( LOGTYPE_DEBUG , "���޵���[%s]�����ļ�[%s][%s] ��[%d]�εȴ�[%d]��" 
                                  , szDailyDate , sFilePkName ,sFilePkName1, i , SLEEP_SECONDS );
                sleep( SLEEP_SECONDS );
                continue;
            }
            else
            {
                SysLog( LOGTYPE_DEBUG , "�ѻ�ȡ����[%s]�����ļ�[%s]" , szDailyDate , sFilePkName );
                break;
            }
        }
        
        if ( i > WAIT_MAX_TIMES )
        {
            return ERROR ;
        }
    }
    
    return NOERR;
}

int ftp_files( MYSQL *conn_ptr )
{
    int i ;
    char acq_list_arr[MAX_ACQ_LIST][12];
    
    /*�����ʼ��*/
    for ( i = 0; i < MAX_ACQ_LIST; i++ )
        memset( acq_list_arr[i], 0x00, sizeof( acq_list_arr[i] ) );

    CHECK ( db_get_acq_list(conn_ptr, acq_list_arr) , "��ȡ������������б�" );
    
    /*�����ʼ��*/
    CHECK ( ftp_file_dirt(acq_list_arr) , "ftpֱ���̻��ļ�ʧ��" );

    /*���µ���״̬ ���Ǽ�->�ѻ�ȡ�ļ�*/
    CHECK ( gen_resp_file_end(conn_ptr) , "��FTP��������ȡ�����ļ�ʧ��" ) ;
        
    return NOERR;
}

/*ֱ���̻��ļ�����*/
int func_gen_direct_file(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "��ȡ�����ļ���ʼ����" );
    
    get_dayend_date( szDailyDate );
    
    iRet = func_ftp_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "ftp�ļ���ʼ��ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*ftp�ļ�������Ŀ¼*/
    iRet = ftp_files(conn_ptr);
     if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "ftp�ļ�������ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }

    SysLog( LOGTYPE_INFO , "��ȡ�����ļ���������" );
    
    return STEP_EXEC_STATUS_SUCC;
}
