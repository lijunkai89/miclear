/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 清分清算系统
 *    SystemName  : 调度步骤-批处理结束
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-批处理结束
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/24         北京           李君凯         创建文档
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

static char  szDailyDate[9]    = { 0 };

#define LOG_SQL_TITLE "批处理结束"

/**
 *变更系统日期表状态，同步日期
 **/
int dayend_end(MYSQL *conn_ptr)
{
    char sql_exec[1024] = { 0 };
    int   iRet               = -1;
    
    /*变更系统日期表状态，同步日期*/
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
 *入口函数
 **/
int func_dayend_end(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "结束批处理开始……" );
    
    get_dayend_date( szDailyDate );

    /*对账*/
    iRet = dayend_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "结束批处理失败" );
        return STEP_EXEC_STATUS_FAIL;
    }

    SysLog( LOGTYPE_INFO , "结束批处理结束……" );
    
    return STEP_EXEC_STATUS_SUCC;
}