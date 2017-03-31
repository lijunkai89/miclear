/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 任务调度主函数
 *    SystemName  : 清分清算系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-任务调度主函数
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/27         北京           李君凯         创建文档
******************************************************************************/

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <dlfcn.h>

#include "mysql.h"
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"


//#define LOG_EXEC_NAME     "dayend"

int          iDayend_Step_Num  = 0;
#define     STEP_MAX_NUM    30          /*步骤数上限*/
#define     PATH_MAX    100          /*程序名称最大长度*/

/*步骤有效标识*/
const int DAYEND_STEP_STATUS_OPEN = 1;
const int DAYEND_STEP_STATUS_CLOSE = 0;

/*步骤依赖性有效标识*/
const int DAYEND_STEP_DEPT_STATUS_OPEN = 1;
const int DAYEND_STEP_DEPT_STATUS_CLOSE = 0;

/*依赖步骤信息*/
typedef struct dayend_dept_step_info
{
    long  lDeptStepId;    /*依赖步骤ID*/
    long  lDistanceTime;  /*依赖步骤间隔时间*/
    int    iDeptStepStatus;  /*依赖步骤状态*/
}dayend_dept_step_info;


/*调度步骤信息*/
typedef struct Steps
{
    long  lId;
    char  sDailyDate[9];
    char  sStepName[PATH_MAX];
    char  sFuncName[50];
    char  sLibName[50];
    int    iStatus;
    char  sBeginTime[21];
    char  sEndTime[21];
    long  lPid;
    int    iDepStepNum;        /*依赖步骤数量*/
    dayend_dept_step_info dayend_dept_step_list[STEP_MAX_NUM];
}stSteps;

stSteps StepsInfo[STEP_MAX_NUM];


/*执行步骤子进程注册信息*/
typedef struct step_register_info
{
    pid_t      pid;
    char      stepname[PATH_MAX];
    int        exitcode;
    int        exitsignal;
}step_register_info;
/*注册信息*/
step_register_info st_step_register_info[STEP_MAX_NUM];
/*注册信息计数器*/
int    step_register_i;

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
 *查询日终步骤信息
 **/
int query_dayend_step_info(MYSQL *conn_ptr)
{
    /*MYSQL变量*/
    MYSQL_RES   *res_ptr;
    MYSQL_ROW   res_row;
    int         row_num = -1, column_num = -1, i , j;
    char        sql_select[1024] = { 0 };
    
    for ( i = 0 ; i < STEP_MAX_NUM ; i++ )
        memset( &StepsInfo[i] , 0x00 , sizeof( stSteps ) );
    
    /*查询待执行步骤信息*/
    sprintf( sql_select, 
             "select t.f_id, "
                    "t.f_dailydate, "
                    "t.f_stepname, "
                    "t.f_funcname, "
                    "t.f_libname, "
                    "t.f_exec_status "
               "from t_sys_dayend_step t "
              "where t.f_status = %d "
              , DAYEND_STEP_STATUS_OPEN
    );
    if ( NOERR != MYSQL_EXEC( conn_ptr , sql_select ) )
    {
        return ERROR;
    }

    res_ptr = mysql_store_result( conn_ptr );
    row_num = mysql_num_rows( res_ptr );
    column_num = mysql_num_fields( res_ptr );

    /*给步骤数赋值*/
    if ( row_num >= STEP_MAX_NUM )
    {
        SysLog( LOGTYPE_ERROR , "步骤数量超限[%d]，实际[%d]步" , STEP_MAX_NUM, row_num );
        return ERROR;
    }
    iDayend_Step_Num = row_num;

    for ( i = 0; i < row_num; i++ )
    {
        res_row = mysql_fetch_row( res_ptr );
        for ( j = 0; j < column_num; j++ )
        {
            switch (j)
            {
            case 0:
              StepsInfo[i].lId = atol( res_row[j] );
              break;
            case 1:
              strcpy( StepsInfo[i].sDailyDate, res_row[j] );
              break;
            case 2:
              strcpy( StepsInfo[i].sStepName, res_row[j] );
              break;
            case 3:
              strcpy( StepsInfo[i].sFuncName, res_row[j] );
              break;
            case 4:
              strcpy( StepsInfo[i].sLibName, res_row[j] );
              break;
            case 5:
              StepsInfo[i].iStatus = atoi( res_row[j] );
              break;
            default:
              SysLog( LOGTYPE_INFO , "查询列数量[%d]超过预计" , j );
              return ERROR;
            }
        }
    }
    
    return NOERR;
}

/**
 *根据步骤ID查询步骤信息
 **/
int find_stepinfo_by_stepid( long lStepId )
{
    int i , iIdx = -1;      /*iIdx-目标步骤在步骤队列中的下标*/

    for( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        if ( StepsInfo[i].lId == lStepId )
        {
            iIdx = i;
        }
    }
    
    if ( i > iDayend_Step_Num )
    {
        SysLog( LOGTYPE_ERROR , "步骤队列中查无此步骤[%ld]" , lStepId );
        iIdx = -1;
    }
    
    return iIdx;
}



/**
 *查询依赖关系
 **/
int query_dept_step_info(MYSQL *conn_ptr)
{
    /*MYSQL变量*/
    MYSQL_RES   *res_ptr;
    MYSQL_ROW   res_row;
    int         row_num = -1, column_num = -1, i , j;
    char        sql_select[1024] = { 0 };
    int         iIdx;  /*步骤数组的下标*/
    dayend_dept_step_info dept_step_info;
    long        lStepId;
    
    sprintf( sql_select,
             "select t3.f_stepid, "
             "       t3.f_depstepid, "
             "       ifnull(t3.f_distancetime, "
             "       0), t2.f_exec_status "
             "  from t_sys_dayend_step      t1, "
             "       t_sys_dayend_step      t2, "
             "       t_sys_dayend_step_depd t3 "
             " where t1.f_id = t3.f_stepid "
             "   and t2.f_id = t3.f_depstepid "
             "   and t1.f_status = %d "
             "   and t2.f_status = %d "
             "   and t3.f_status = %d "
             , DAYEND_STEP_STATUS_OPEN
             , DAYEND_STEP_STATUS_OPEN
             , DAYEND_STEP_DEPT_STATUS_OPEN
    );
    
    if ( NOERR != MYSQL_EXEC( conn_ptr , sql_select ) )
    {
        return ERROR;
    }

    res_ptr = mysql_store_result( conn_ptr );
    row_num = mysql_num_rows( res_ptr );
    column_num = mysql_num_fields( res_ptr );

    for ( i = 0; i < row_num; i++ )
    {
        res_row = mysql_fetch_row( res_ptr );
        for ( j = 0; j < column_num; j++ )
        {
            switch (j)
            {
            case 0:
              lStepId = atol( res_row[j] );
              break;
            case 1:
              dept_step_info.lDeptStepId = atol( res_row[j] );
              break;
            case 2:
              dept_step_info.lDistanceTime = atol( res_row[j] );
              break;
            case 3:
              dept_step_info.iDeptStepStatus = atol( res_row[j] );
              break;
            default:
              SysLog( LOGTYPE_INFO , "查询列数量[%d]超过预计" , j );
              return ERROR;
            }
        }
        
        /*完善步骤信息-增加依赖步骤信息*/
        iIdx = find_stepinfo_by_stepid( lStepId );
        if ( iIdx >= 0 )
        {
            StepsInfo[iIdx].dayend_dept_step_list[StepsInfo[iIdx].iDepStepNum++] = dept_step_info;
        }
    }
    
    return NOERR;
}

/**
 *校验依赖步骤执行完成否
 **/
int check_step_depend( stSteps stSigSteps )
{
    int i;

    for ( i = 0 ; i < stSigSteps.iDepStepNum ; i++ )
    {
        if ( STEP_EXEC_STATUS_SUCC != stSigSteps.dayend_dept_step_list[i].iDeptStepStatus )
        {
                return ERROR ;
        }
    }
    
    return NOERR ;
}


/**
 *更新步骤状态
 *        未执行 -> 正在执行
 **/
int update_dayend_step_start_status( MYSQL *conn_ptr, stSteps stSigSteps )
{
    /*MYSQL变量*/
    char        sql_exec[1024] = { 0 };
    
    sprintf( sql_exec,
             "update t_sys_dayend_step t "
             "   set t.f_exec_status = %d , "
             "       t.f_begintime = date_format(sysdate(),'%%Y-%%m-%%d %%h:%%i:%%s') , "
             "       t.f_pid = %d "
             " where t.f_id = %d "
             "   and t.f_exec_status = %d "
             , STEP_EXEC_STATUS_DOING
             , getpid()
             , stSigSteps.lId
             , STEP_EXEC_STATUS_INIT
    );
    
    if( NOERR != MYSQL_EXEC( conn_ptr , sql_exec ) )
    {
        return ERROR ;
    }

    return NOERR ;
}


/**
 *更新步骤状态
 *        未执行 -> 正在执行
 **/
int update_dayend_step_end_status( MYSQL *conn_ptr, stSteps stSigSteps , int iResult )
{
    /*MYSQL变量*/
    char        sql_exec[1024] = { 0 };
    
    sprintf( sql_exec,
             "update t_sys_dayend_step t "
             "   set t.f_exec_status = %d , "
             "       t.f_endtime = date_format(sysdate(),'%%Y-%%m-%%d %%h:%%i:%%s') "
             " where t.f_id = %d "
             "   and t.f_exec_status = %d "
             , iResult
             , stSigSteps.lId
             , STEP_EXEC_STATUS_DOING
    );
    
    if( NOERR != MYSQL_EXEC( conn_ptr , sql_exec ) )
    {
        return ERROR ;
    }

    return NOERR ;
}


/**
 *执行步骤
 **/
void exec_step( stSteps stSigSteps )
{
    char lock_file_name[50]    = { 0 } ;
    void *handle              = NULL;        /*动态库句柄*/
    step_func_api func        = NULL;        /*入口函数地址*/
    int    iRet                = 0;
    MYSQL  conn_ptr ;
    
    /*创建步骤文件锁*/
    sprintf( lock_file_name , "%s_%d" , LOCK_FILE_PREFIX_STEP , stSigSteps.lId );

    /*步骤加锁*/
    lock_file( lock_file_name );
    
    /*登记步骤名*/
    put_dayend_stepname( stSigSteps.sStepName );

    /*加载步骤的动态库*/
    if ( NULL == ( handle = dlopen( stSigSteps.sLibName , RTLD_LAZY ) ) )
    {
        SysLog( LOGTYPE_ERROR , "打开步骤动态库[%s]失败 err[%s]" , 
                stSigSteps.sLibName , dlerror() );
        exit( _EXIT_FAILURE );
    }
    
    /*获取函数指针*/
    if ( NULL == ( func = ( step_func_api )dlsym( handle , stSigSteps.sFuncName ) ) )
    {
        SysLog( LOGTYPE_ERROR , "获取步骤函数[%s]入口地址失败 [%s]" , 
                stSigSteps.sFuncName , strerror( errno ) );
        dlclose( handle );
        exit( _EXIT_FAILURE );
    }

    SysLog( LOGTYPE_INFO , "步骤{%s]开始 执行进程[%d]" , stSigSteps.sStepName , getpid() );
    
    /*每一步建立步骤数据库连接*/
    if ( NOERR != dbopen(&conn_ptr) )
    {
        exit( _EXIT_FAILURE );
    }
    
    /*更新步骤状态：初始化-正在执行*/
    if ( ( iRet = update_dayend_step_start_status( &conn_ptr, stSigSteps ) ) != NOERR )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }

    iRet = func( &conn_ptr );

    dlclose( handle );

    if ( STEP_EXEC_STATUS_SUCC != iRet && STEP_EXEC_STATUS_FAIL != iRet )
    {
        SysLog( LOGTYPE_ERROR , "执行结果未知" );
        iRet = STEP_EXEC_STATUS_UNKNOWN;
    }

    /*更新步骤状态：正在执行-执行结果*/
    if ( ( iRet = update_dayend_step_end_status( &conn_ptr, stSigSteps , iRet ) ) != NOERR )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    SysLog( LOGTYPE_INFO , "步骤{%s]完成" , stSigSteps.sStepName );
    
    /*断开步骤数据库连接*/
    dbclose(&conn_ptr);
    
    exit( iRet );
}


/**
 *注册信息初始化
 **/
void step_register_init()
{
    int  i = 0;
    
    step_register_i = 0;
    for ( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        memset( &st_step_register_info[i] , 0x00 , sizeof( step_register_info ) );
    }
}


/**
 *登记注册信息初
 **/
void step_register_register ( stSteps stSigSteps , pid_t pid )
{
    st_step_register_info[step_register_i].pid = pid;
    strcpy( st_step_register_info[step_register_i].stepname , stSigSteps.sStepName );
    step_register_i++;
}


/**
 *查询注册信息初
 **/
int step_register_select( pid_t pid , int *iDx )
{
    int  i = 0;
    
    for ( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        if ( st_step_register_info[i].pid == pid )
        {
            *iDx = i;
            return ERROR;
        }
    }
    
    return NOERR;
}


/**
 *注册信息非空校验
 *返回：      
 *      ERROR - 非空，继续等待
 *      NOERR - 空，无需等待
 **/
int step_register_chk()
{
    int  i = 0;

    for ( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        if ( st_step_register_info[i].pid )
        {
            return ERROR;
        }
    }
    
    return NOERR;
}


/**
 *清除对应Pid的注册信息
 **/
void step_register_clear( pid_t pid )
{
    int  i = 0;

    for ( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        if ( st_step_register_info[i].pid == pid )
        {
            memset( &st_step_register_info[i] , 0x00 , sizeof( step_register_info ) );
            break;
        }
    }
}



/**
 *执行单步步骤
 *      stSigSteps - 步骤ID
 *      ch_force   - 强制单步执行标识
 **/
void exec_sig_steps( stSteps stSigSteps , int ch_force )
{
    pid_t     pid    = 0;
    
    if ( !ch_force )
    {
        /*查询依赖步骤是否执行完成*/
        if ( check_step_depend( stSigSteps ) )
        {
            return ;
        }
    }
    
    pid = fork();
    if ( pid == 0 )
    {
        /*子进程执行步骤*/

        /*单步执行步骤*/
        exec_step( stSigSteps );
    }
    else if ( pid > 0 )
    {
        /*父进程登记步骤执行信息*/
        step_register_register( stSigSteps , pid );
    }
    else
    {
        /*创建子进程执行失败*/
        SysLog( LOGTYPE_ERROR , "执行步骤{%s]创建子进程失败" , stSigSteps.sStepName );
    }
}


/**
 *执行所有步骤
 **/
void exec_all_steps()
{
    int i ;

    /*执行每个步骤*/
    for ( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        /*如果此步骤未被执行则执行*/
        if ( STEP_EXEC_STATUS_INIT == StepsInfo[i].iStatus )
        {
            exec_sig_steps( StepsInfo[i] , 0 );
        }
    }

}



/**
 *执行所有步骤
 **/
void wait_exec_steps()
{
    int   exec_status    = 0;        /*子进程执行状态*/
    pid_t  pid            = 0;
    int   iDx            = 0;        /*注册信息数组角标*/
    
    while(1)
    {
        pid = wait( &exec_status );

        if ( -1 == pid )
        {
            if ( EINTR == errno )
            {
                continue;          /*被信号中断,继续等待*/
            }
            else if ( ECHILD == errno )
            {
                break;            /*所有的子进程均已回收*/
            }
            else
            {
                SysLog( LOGTYPE_ERROR , "主进程等待回收子进程失败[%d]-[%s]" ,
                        errno , strerror( errno ) );
            }
        }
        else
        {
            /*根据PID查询注册信息*/
            step_register_select( pid , &iDx );

            if ( WIFEXITED( exec_status ) )
            {
                /*正常结束(通过正常的return或exit结束)*/
                st_step_register_info[iDx].exitcode = WEXITSTATUS( exec_status );
                SysLog( LOGTYPE_INFO , "日终任务[%s]执行结果[%d]" ,
                        st_step_register_info[iDx].stepname , 
                        st_step_register_info[iDx].exitcode );
            }
            else 
            {
                if ( WIFSIGNALED( exec_status ) )
                {
                    st_step_register_info[iDx].exitsignal = WTERMSIG( exec_status );
                    SysLog( LOGTYPE_ERROR , "日终步骤[%s][执行进程%d]收到信号[%d]而导致非法结束" ,
                            st_step_register_info[iDx].stepname , 
                            pid , 
                            st_step_register_info[iDx].exitsignal );
                }
                else if ( WIFSTOPPED( exec_status ) )
                {
                    st_step_register_info[iDx].exitsignal = WSTOPSIG( exec_status );
                    SysLog( LOGTYPE_ERROR , "日终步骤[%s]被信号[%d]停止" ,
                            st_step_register_info[iDx].stepname , 
                            st_step_register_info[iDx].exitsignal );
                }
                else if ( WIFCONTINUED( exec_status ) )
                {
                    SysLog( LOGTYPE_ERROR , "日终步骤[%s]被重新启动执行" ,
                            st_step_register_info[iDx].stepname );
                }
                else
                {
                    SysLog( LOGTYPE_ERROR , "收到日终步骤[%s]的非法状态变更通知" ,
                            st_step_register_info[iDx].stepname );
                }
            }
        }
        
        /*清除注册信息*/
        step_register_clear( pid );
    }
}

void main( int argc , char** argv )
{
    int    ch = 0, iRet = NOERR;
    int    ch_force = 0, ch_step = 0;
    long   lStepId     = 0;
    int    iIdx = -1;      /*iIdx-目标步骤在步骤队列中的下标*/
    
    MYSQL  conn_ptr ;
    
    /*登记步骤名*/
    put_dayend_stepname( argv[0] );
    
    while( -1 != ( ch = getopt( argc, argv, "hn:f" ) ) )
    {
        switch( ch )
        {
        case 'h':
            fprintf( stderr , "Usage: 程序名 -n <单步执行步骤名> -f <强制执行，忽略依赖关系> -h <help>\n" );
            exit( _EXIT_SUCCESS );
        case 'n':
            ch_step = 1;
            lStepId = atoi( optarg ) ;
            break;
        case 'f':
            ch_force = 1;
            break;
        default:
            fprintf( stderr , "Usage: 程序名 -n <单步执行步骤名> -f <强制执行，忽略依赖关系> -h <help>\n" );
            exit( _EXIT_FAILURE );
        }
    }
    
    /*建立文件锁，防止屏凡*/
    iRet = lock_file( LOCK_FILE_DAYENDMAIN );
    if ( iRet != NOERR )
    {
        exit( _EXIT_SUCCESS );
    }
     
    /*主进程连接数据库*/
    if ( NOERR != dbopen(&conn_ptr) )
    {
        exit( _EXIT_FAILURE );
    }

    /*查询系统信息*/
    if ( NOERR != query_sysinfo( &conn_ptr ) )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    if ( SYS_STATUS_END != get_sys_status( ) )
    {
        SysLog( LOGTYPE_INFO , "系统非日终状态" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*查询日终步骤信息*/
    if ( NOERR != query_dayend_step_info( &conn_ptr ) )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }

    /*查询依赖关系*/
    if ( NOERR != query_dept_step_info( &conn_ptr ) )
    {
        SysLog( LOGTYPE_INFO , "before wait_step_exec" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*断开步骤数据库连接*/
    dbclose(&conn_ptr);

    /*初始化注册信息*/
    step_register_init();

    if ( ch_step )
    {
        /*根据步骤ID查询步骤信息*/
        iIdx = find_stepinfo_by_stepid( lStepId );
        
        exec_sig_steps( StepsInfo[iIdx] , ch_force );
    }
    else
    {
        exec_all_steps();
    }
    
    if ( step_register_chk() )
    {
        /*等待所有子进程返回*/
        SysLog( LOGTYPE_INFO , "before wait_step_exec" );
        wait_exec_steps();
        SysLog( LOGTYPE_INFO , "after wait_step_exec" );
    }

    SysLog( LOGTYPE_INFO , "调度程序退出..." );

    exit( iRet );
}
