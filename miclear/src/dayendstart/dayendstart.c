/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 任务调度主函数
 *    SystemName  : 清分清算系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-日切主函数
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/11         北京           李君凯         创建文档
******************************************************************************/

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "mysql.h"
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"

static char  szDailyDate[9]    = { 0 };

#define LOG_SQL_TITLE "批处理开始"

/**
 *查询系统信息
 **/
int query_sysinfo(MYSQL *conn_ptr)
{
    /*MYSQL变量*/
    MYSQL_RES   *res_ptr;
    MYSQL_ROW   res_row;
    int         row_num = -1, column_num = -1, i , j;
    char        sql_select[1024] = { 0 };
    
    /*查询系统日期信息*/
    sprintf( sql_select , "%s" , "select t.f_accdate, t.f_status, t.f_daily_date from t_sys_daily_info t" );
    if ( NOERR != MYSQL_EXEC( conn_ptr , sql_select ) )
    {
        return ERROR;
    }

    res_ptr = mysql_store_result( conn_ptr );
    row_num = mysql_num_rows( res_ptr );
    if ( row_num != 1 )
    {
        SysLog( LOGTYPE_ERROR , "系统日期表配置行数[%d]不正确" , row_num );
        return ERROR;
    }
    
    /*全局变量赋值*/
    res_row = mysql_fetch_row( res_ptr );
    column_num = mysql_num_fields( res_ptr );
    for ( i = 0; i < column_num; i++ )
    {
        switch (i)
        {
        case 0:
          pub_logic_date( res_row[i] );
          break;
        case 1:
          put_sys_status( atoi(res_row[i]) );
          break;
        case 2:
          put_dayend_date( res_row[i] );
          break;
        default:
          SysLog( LOGTYPE_INFO , "查询列数量[%d]超过预计" , i );
          return ERROR;
        }
    }
    
    return NOERR ;
}

/**
 *调用日切开始函数
 **/
int func_dayend_start( MYSQL *conn_ptr )
{
    /*MYSQL变量*/
    char        sql_exec[1024] = { 0 };
    char        sdate_6[7]     = { 0 };
    char        sfile_name_date[7] = { 0 };
    char        seq[21]        = { 0 };
    int         iRet = -1;
    
    /* 获取系统日终日期 */
    get_dayend_date( szDailyDate );
    strncpy( sdate_6, szDailyDate+2, 6 );
    
    /*更新系统日期和状态*/
    sprintf( sql_exec,
             "update t_sys_daily_info t "
             "set t.f_status = %d, "
             "    t.f_accdate = date_format(adddate(\'%s\', 1), \'%%Y%%m%%d\') "
             , SYS_STATUS_END
             , szDailyDate
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet <= 0 )
    {
        return ERROR ;
    }
    
    /*备份步骤日志*/
    memset( sql_exec, 0x00, sizeof( sql_exec ) );
    sprintf( sql_exec,
            "INSERT INTO t_sys_dayend_step_log ( "
            "  f_id, "
            "  f_dailydate, "
            "  f_stepname, "
            "  f_exec_status, "
            "  f_status, "
            "  f_begintime, "
            "  f_endtime, "
            "  f_pid, "
            "  f_comments "
            "  ) "
            "  SELECT concat(t1.f_dailydate, t1.f_id, t1.f_pid) , "
            "          t1.f_dailydate, "
            "          t1.f_stepname, "
            "          t1.f_exec_status, "
            "          t1.f_status, "
            "          t1.f_begintime, "
            "          t1.f_endtime, "
            "          t1.f_pid, "
            "          t1.f_comments "
            "     FROM t_sys_dayend_step t1 "
            "    WHERE t1.f_dailydate = \'%s\'  "
            "  ON DUPLICATE KEY UPDATE f_dailydate = t1.f_dailydate, "
            "         f_stepname = t1.f_stepname, "
            "         f_exec_status = t1.f_exec_status, "
            "         f_status = t1.f_status, "
            "         f_begintime = t1.f_begintime, "
            "         f_endtime = t1.f_endtime, "
            "         f_pid = t1.f_pid, "
            "         f_comments = t1.f_comments "
            , szDailyDate
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR ;
    }
    
    
    /*初始化步骤表*/
    memset( sql_exec, 0x00, sizeof( sql_exec ) );
    sprintf( sql_exec,
            " UPDATE t_sys_dayend_step "
            "    SET f_dailydate = \'%s\', "
            "        f_exec_status = %d, "
            "        f_begintime = \'\', "
            "        f_endtime = \'\', "
            "        f_pid = 0, "
            "        f_comments = \'\' "
            "  WHERE f_status = 1 "
            , szDailyDate
            , STEP_EXEC_STATUS_INIT
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet <= 0 )
    {
        return ERROR ;
    }

    /*获取文件名*/
    memset( sfile_name_date, 0x00, sizeof( sfile_name_date ) );
    sprintf( sfile_name_date, "IND%s01ACOM", sdate_6);
    
    /*获取序列*/
    memset( seq, 0x00, sizeof( seq ) );
    gen_dtl_id(conn_ptr, "seq_liquidate_file_id", seq);
    
    /*登记代付回执文件任务*/ 
    sprintf( sql_exec,
            " INSERT INTO t_liquidate_file ( "
            "  f_id, "
            "  f_gen_dt, "
            "  f_file_date, "
            "  f_file_name, "
            "  f_channel_no) "
            " SELECT \'%s\', "
            "        date_format(sysdate(),\'%%Y-%%m-%%d %%H:%%i:%%s\'), "
            "        \'%s\', "
            "        \'%s\', "
            "        %d "
            " ON DUPLICATE KEY "
            " UPDATE f_gen_dt = date_format(sysdate(),\'%%Y-%%m-%%d %%H:%%i:%%s\') "
            , seq
            , szDailyDate
            , sfile_name_date
            , CHANNEL_CUP_DAIFU
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR ;
    }
    
    
    /*获取文件名*/
    memset( sfile_name_date, 0x00, sizeof( sfile_name_date ) );
    sprintf( sfile_name_date, "IND%s32ACOMA", sdate_6);
    
    /*获取序列*/
    memset( seq, 0x00, sizeof( seq ) );
    gen_dtl_id(conn_ptr, "seq_liquidate_file_id", seq);
    
    /*登记直连清算文件任务*/
    memset( sql_exec, 0x00, sizeof( sql_exec ) );
    sprintf( sql_exec,
            " INSERT INTO t_liquidate_file ( "
            "  f_id, "
            "  f_gen_dt, "
            "  f_file_date, "
            "  f_file_name, "
            "  f_channel_no) "
            " SELECT \'%s\', "
            "        date_format(sysdate(),\'%%Y-%%m-%%d %%H:%%i:%%s\'), "
            "        \'%s\', "
            "        \'%s\', "
            "        %d "
            " ON DUPLICATE KEY "
            " UPDATE f_gen_dt = date_format(sysdate(),\'%%Y-%%m-%%d %%H:%%i:%%s\') "
            , seq
            , szDailyDate
            , sfile_name_date
            , CHANNEL_CUP_DIRECT
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR ;
    }

    return NOERR ;
}

void main( int argc , char** argv )
{
    int    ch = 0, iRet = NOERR;    
    MYSQL  conn_ptr ;
    
    /*登记步骤名*/
    put_dayend_stepname( argv[0] );
    
    while( -1 != ( ch = getopt( argc, argv, "hn:f" ) ) )
    {
        switch( ch )
        {
        case 'h':
            fprintf( stderr , "本程序实现日期切割，开启当天日中处理-h <help>\n" );
            exit( _EXIT_SUCCESS );
        default:
            fprintf( stderr , "Usage: 程序名  -h <help>\n" );
            exit( _EXIT_FAILURE );
        }
    }
    
    SysLog( LOGTYPE_INFO , "程序开始..." );
    
    /*建立文件锁，防止屏凡*/
    iRet = lock_file( LOCK_FILE_DAYENDSTART );
    if ( iRet != NOERR )
    {
        exit( _EXIT_SUCCESS );
    }
    
    /*主进程连接数据库*/
    if ( NOERR != dbopen(&conn_ptr) )
    {
        SysLog( LOGTYPE_INFO , "打开数据库失败" );
        exit( _EXIT_FAILURE );
    }

    /*查询系统信息*/
    if ( NOERR != query_sysinfo( &conn_ptr ) )
    {
        SysLog( LOGTYPE_INFO , "查询系统信息失败" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    if ( SYS_STATUS_END == get_sys_status( ) )
    {
        SysLog( LOGTYPE_INFO , "系统已是日终状态" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*会计日期切到下一日，变更日终状态*/
    if ( NOERR != func_dayend_start( &conn_ptr ) )
    {
        SysLog( LOGTYPE_INFO , "批处理开始失败" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*断开步骤数据库连接*/
    dbclose(&conn_ptr);

    SysLog( LOGTYPE_INFO , "程序退出..." );

    exit( iRet );
}
