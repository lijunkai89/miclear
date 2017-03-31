/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������ϵͳ
 *    SystemName  : ���Ȳ���-���������
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-���������
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/24         ����           �����         �����ĵ�
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

static char  szDailyDate[9]    = { 0 };

#define LOG_SQL_TITLE "���������"

/**
 *���ϵͳ���ڱ�״̬��ͬ������
 **/
int dayend_end(MYSQL *conn_ptr)
{
    char sql_exec[1024] = { 0 };
    int   iRet               = -1;
    
    /*���ϵͳ���ڱ�״̬��ͬ������*/
    sprintf( sql_exec,
            " update t_sys_daily_info t "
            " set t.f_status = %d, "
            " t.f_daily_date = t.f_accdate "
            " where t.f_status = %d "
            , SYS_STATUS_NOM
            , SYS_STATUS_END
    );
    
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet <= 0 )
    {
        return ERROR;
    }

    return NOERR;
}

/**
 *��ں���
 **/
int func_dayend_end(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "����������ʼ����" );
    
    get_dayend_date( szDailyDate );

    /*����*/
    iRet = dayend_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "����������ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }

    SysLog( LOGTYPE_INFO , "�����������������" );
    
    return STEP_EXEC_STATUS_SUCC;
}