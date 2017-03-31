/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������������
 *    SystemName  : �������ϵͳ
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-����������
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/11         ����           �����         �����ĵ�
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

#define LOG_SQL_TITLE "������ʼ"

/**
 *��ѯϵͳ��Ϣ
 **/
int query_sysinfo(MYSQL *conn_ptr)
{
    /*MYSQL����*/
    MYSQL_RES   *res_ptr;
    MYSQL_ROW   res_row;
    int         row_num = -1, column_num = -1, i , j;
    char        sql_select[1024] = { 0 };
    
    /*��ѯϵͳ������Ϣ*/
    sprintf( sql_select , "%s" , "select t.f_accdate, t.f_status, t.f_daily_date from t_sys_daily_info t" );
    if ( NOERR != MYSQL_EXEC( conn_ptr , sql_select ) )
    {
        return ERROR;
    }

    res_ptr = mysql_store_result( conn_ptr );
    row_num = mysql_num_rows( res_ptr );
    if ( row_num != 1 )
    {
        SysLog( LOGTYPE_ERROR , "ϵͳ���ڱ���������[%d]����ȷ" , row_num );
        return ERROR;
    }
    
    /*ȫ�ֱ�����ֵ*/
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
          SysLog( LOGTYPE_INFO , "��ѯ������[%d]����Ԥ��" , i );
          return ERROR;
        }
    }
    
    return NOERR ;
}

/**
 *�������п�ʼ����
 **/
int func_dayend_start( MYSQL *conn_ptr )
{
    /*MYSQL����*/
    char        sql_exec[1024] = { 0 };
    char        sdate_6[7]     = { 0 };
    char        sfile_name_date[7] = { 0 };
    char        seq[21]        = { 0 };
    int         iRet = -1;
    
    /* ��ȡϵͳ�������� */
    get_dayend_date( szDailyDate );
    strncpy( sdate_6, szDailyDate+2, 6 );
    
    /*����ϵͳ���ں�״̬*/
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
    
    /*���ݲ�����־*/
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
    
    
    /*��ʼ�������*/
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

    /*��ȡ�ļ���*/
    memset( sfile_name_date, 0x00, sizeof( sfile_name_date ) );
    sprintf( sfile_name_date, "IND%s01ACOM", sdate_6);
    
    /*��ȡ����*/
    memset( seq, 0x00, sizeof( seq ) );
    gen_dtl_id(conn_ptr, "seq_liquidate_file_id", seq);
    
    /*�ǼǴ�����ִ�ļ�����*/ 
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
    
    
    /*��ȡ�ļ���*/
    memset( sfile_name_date, 0x00, sizeof( sfile_name_date ) );
    sprintf( sfile_name_date, "IND%s32ACOMA", sdate_6);
    
    /*��ȡ����*/
    memset( seq, 0x00, sizeof( seq ) );
    gen_dtl_id(conn_ptr, "seq_liquidate_file_id", seq);
    
    /*�Ǽ�ֱ�������ļ�����*/
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
    
    /*�Ǽǲ�����*/
    put_dayend_stepname( argv[0] );
    
    while( -1 != ( ch = getopt( argc, argv, "hn:f" ) ) )
    {
        switch( ch )
        {
        case 'h':
            fprintf( stderr , "������ʵ�������и�����������д���-h <help>\n" );
            exit( _EXIT_SUCCESS );
        default:
            fprintf( stderr , "Usage: ������  -h <help>\n" );
            exit( _EXIT_FAILURE );
        }
    }
    
    SysLog( LOGTYPE_INFO , "����ʼ..." );
    
    /*�����ļ�������ֹ����*/
    iRet = lock_file( LOCK_FILE_DAYENDSTART );
    if ( iRet != NOERR )
    {
        exit( _EXIT_SUCCESS );
    }
    
    /*�������������ݿ�*/
    if ( NOERR != dbopen(&conn_ptr) )
    {
        SysLog( LOGTYPE_INFO , "�����ݿ�ʧ��" );
        exit( _EXIT_FAILURE );
    }

    /*��ѯϵͳ��Ϣ*/
    if ( NOERR != query_sysinfo( &conn_ptr ) )
    {
        SysLog( LOGTYPE_INFO , "��ѯϵͳ��Ϣʧ��" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    if ( SYS_STATUS_END == get_sys_status( ) )
    {
        SysLog( LOGTYPE_INFO , "ϵͳ��������״̬" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*��������е���һ�գ��������״̬*/
    if ( NOERR != func_dayend_start( &conn_ptr ) )
    {
        SysLog( LOGTYPE_INFO , "������ʼʧ��" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*�Ͽ��������ݿ�����*/
    dbclose(&conn_ptr);

    SysLog( LOGTYPE_INFO , "�����˳�..." );

    exit( iRet );
}
