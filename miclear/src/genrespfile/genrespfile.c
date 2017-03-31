/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������ϵͳ
 *    SystemName  : ���Ȳ���-��ȡ���������ļ�
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-��ȡ���������ļ�
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/09         ����           �����         �����ĵ�
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

//#define sftp_drt_sh     "ftp_get_cup_file_drt.sh"
#define sftp_ind_sh     "ftp_get_cup_file_ind.sh"
#define ftp_log      "ftp_get_cup_file.log"
//#define sunzip_drt_sh     "cup_file_drt_unzip.sh"
#define sunzip_ind_sh     "cup_file_ind_unzip.sh"
//#define szip_check_sh     "deal_check_files.sh"

static char  szDailyDate[9]    = { 0 };

int func_ftp_init(MYSQL *conn_ptr)
{
    int iRet = 0;
    char  szSql[2048]    = { 0 };
    
    memset(szSql, 0x00, sizeof(szSql));
    sprintf( szSql , 
            " update t_liquidate_file t "
            "    set t.f_load_time = null, "
            "        t.f_status = %d "
            "  where t.f_file_date = \'%s\' "
            , LIQUIDATE_FILE_STATUS_INIT
            , szDailyDate
    );
    
    if ( NOERR != MYSQL_EXEC_STEP( conn_ptr , szSql ) )
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
            , LIQUIDATE_FILE_STATUS_GEN_SUCC 
            , szDailyDate 
            , LIQUIDATE_FILE_STATUS_INIT
    );
    
    if ( NOERR != MYSQL_EXEC_STEP1( conn_ptr , szSql ) )
    {
        return ERROR;
    }
    
    return NOERR;
}

/**
 *ftp��ȡ���ռ����̻������ļ�
 **/
static int ftp_file_ind()
{
    int    status        = -1;
    char   commond[255]  = { 0 };
    int    i;
    char   sFilePkName[255]  = { 0 };        /*Ŀ���ļ�����*/
    char   sFilePkNameAll[255]  = { 0 };     /*Ŀ���ļ�ȫ��*/
    char   szDailyDate_yy[7]  = { 0 };
    FILE   *fp_chk_file  = NULL;

    /*��ȡ6λ���� YYMMDD*/
    strncpy( szDailyDate_yy , szDailyDate + 2 , 6 );
    
    //�����ļ�ȫ����������·��
    sprintf( sFilePkName , "IND%s01ACOM.Z" , szDailyDate_yy ) ,
    
    //�����ļ�ȫ����������·��
    sprintf( sFilePkNameAll , "%s/%s/%s" , getenv( "LIQUIDATE_FILE_LOCAL" ) , szDailyDate , sFilePkName );
    SysLog( LOGTYPE_DEBUG , "�����ļ���[%s]" , sFilePkNameAll );
    
    /*ƴ�ӵ���ѹ���ļ�����·��*/
    sprintf( commond , "%s %s %s >> %s/%s_%s 2>&1" , 
              sftp_ind_sh , szDailyDate , sFilePkName , 
              getenv( "LOGDIR" ) , szDailyDate , ftp_log );
    SysLog( LOGTYPE_DEBUG , "%s" , commond );
    
    #if 1
    //�ȴ�ftp�ļ�
    for ( i = 1; i <= WAIT_MAX_TIMES ; i++ )
    {
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
    
        //��ʱC�����ж��ļ��Ƿ�ftp�ɹ�
        if ( (fp_chk_file = fopen( sFilePkNameAll , "rb" ) ) == NULL )
        {
            SysLog( LOGTYPE_DEBUG , "���޵���[%s]�����ļ�[%s] ��[%d]�εȴ�[%d]��" 
                                  , szDailyDate , sFilePkName , i , SLEEP_SECONDS );
            sleep( SLEEP_SECONDS );
            continue;
        }
        else
        {
            SysLog( LOGTYPE_DEBUG , "�ѻ�ȡ����[%s]�����ļ�[%s]" , szDailyDate , sFilePkName );
            
            //��ѹ�����ļ�
            memset( commond , 0x00 , sizeof( commond ) );
            sprintf( commond , "%s %s %s >> %s/%s_%s 2>&1" , 
                      sunzip_ind_sh , szDailyDate , sFilePkName , 
                      getenv( "LOGDIR" ) , szDailyDate , ftp_log );
            SysLog( LOGTYPE_DEBUG , "%s" , commond );
            
            system( commond );
            
            break;
        }
    }
    #endif
    
    if ( i > WAIT_MAX_TIMES )
    {
        return ERROR ;
    }
    
    return NOERR;
}

int ftp_files( MYSQL *conn_ptr )
{
    //CHECK ( ftp_file_drt() , "ftpֱ���̻��ļ�ʧ��" );

    CHECK ( ftp_file_ind() , "ftp�����̻��ļ�ʧ��" );

    /*���µ���״̬ ���Ǽ�->�ѻ�ȡ�ļ�*/
    CHECK ( gen_resp_file_end(conn_ptr) , "��FTP��������ȡ�����ļ�ʧ��" ) ;
    SysLog( LOGTYPE_ERROR, "ִ�гɹ�" );
    return NOERR;
}


int func_gen_resp_file(MYSQL *conn_ptr)
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
