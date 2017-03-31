/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 清分清算系统
 *    SystemName  : 调度步骤-获取代付对账文件
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-获取代付对账文件
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/09         北京           李君凯         创建文档
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
    
    SysLog( LOGTYPE_INFO , "初始化银联对账单结束" ) ;  
    
    return NOERR;
}


/**
 *文件已获取，更新任务状态
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
 *ftp获取当日间连商户对账文件
 **/
static int ftp_file_ind()
{
    int    status        = -1;
    char   commond[255]  = { 0 };
    int    i;
    char   sFilePkName[255]  = { 0 };        /*目标文件包名*/
    char   sFilePkNameAll[255]  = { 0 };     /*目标文件全名*/
    char   szDailyDate_yy[7]  = { 0 };
    FILE   *fp_chk_file  = NULL;

    /*截取6位日期 YYMMDD*/
    strncpy( szDailyDate_yy , szDailyDate + 2 , 6 );
    
    //对账文件全名：含绝对路径
    sprintf( sFilePkName , "IND%s01ACOM.Z" , szDailyDate_yy ) ,
    
    //对账文件全名：含绝对路径
    sprintf( sFilePkNameAll , "%s/%s/%s" , getenv( "LIQUIDATE_FILE_LOCAL" ) , szDailyDate , sFilePkName );
    SysLog( LOGTYPE_DEBUG , "间连文件名[%s]" , sFilePkNameAll );
    
    /*拼接当日压缩文件绝对路径*/
    sprintf( commond , "%s %s %s >> %s/%s_%s 2>&1" , 
              sftp_ind_sh , szDailyDate , sFilePkName , 
              getenv( "LOGDIR" ) , szDailyDate , ftp_log );
    SysLog( LOGTYPE_DEBUG , "%s" , commond );
    
    #if 1
    //等待ftp文件
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
    
        //暂时C代码判断文件是否ftp成功
        if ( (fp_chk_file = fopen( sFilePkNameAll , "rb" ) ) == NULL )
        {
            SysLog( LOGTYPE_DEBUG , "查无当日[%s]对账文件[%s] 第[%d]次等待[%d]秒" 
                                  , szDailyDate , sFilePkName , i , SLEEP_SECONDS );
            sleep( SLEEP_SECONDS );
            continue;
        }
        else
        {
            SysLog( LOGTYPE_DEBUG , "已获取当日[%s]对账文件[%s]" , szDailyDate , sFilePkName );
            
            //解压当日文件
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
    //CHECK ( ftp_file_drt() , "ftp直连商户文件失败" );

    CHECK ( ftp_file_ind() , "ftp间连商户文件失败" );

    /*更新单据状态 初登记->已获取文件*/
    CHECK ( gen_resp_file_end(conn_ptr) , "从FTP服务器获取银联文件失败" ) ;
    SysLog( LOGTYPE_ERROR, "执行成功" );
    return NOERR;
}


int func_gen_resp_file(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "获取银联文件开始……" );
    
    get_dayend_date( szDailyDate );
    
    iRet = func_ftp_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "ftp文件初始化失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*ftp文件到本地目录*/
    iRet = ftp_files(conn_ptr);
     if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "ftp文件到本地失败" );
        return STEP_EXEC_STATUS_FAIL;
    }

    SysLog( LOGTYPE_INFO , "获取银联文件结束……" );
    
    return STEP_EXEC_STATUS_SUCC;
}
