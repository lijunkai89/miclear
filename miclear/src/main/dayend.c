/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������������
 *    SystemName  : �������ϵͳ
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-�������������
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/27         ����           �����         �����ĵ�
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
#define     STEP_MAX_NUM    30          /*����������*/
#define     PATH_MAX    100          /*����������󳤶�*/

/*������Ч��ʶ*/
const int DAYEND_STEP_STATUS_OPEN = 1;
const int DAYEND_STEP_STATUS_CLOSE = 0;

/*������������Ч��ʶ*/
const int DAYEND_STEP_DEPT_STATUS_OPEN = 1;
const int DAYEND_STEP_DEPT_STATUS_CLOSE = 0;

/*����������Ϣ*/
typedef struct dayend_dept_step_info
{
    long  lDeptStepId;    /*��������ID*/
    long  lDistanceTime;  /*����������ʱ��*/
    int    iDeptStepStatus;  /*��������״̬*/
}dayend_dept_step_info;


/*���Ȳ�����Ϣ*/
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
    int    iDepStepNum;        /*������������*/
    dayend_dept_step_info dayend_dept_step_list[STEP_MAX_NUM];
}stSteps;

stSteps StepsInfo[STEP_MAX_NUM];


/*ִ�в����ӽ���ע����Ϣ*/
typedef struct step_register_info
{
    pid_t      pid;
    char      stepname[PATH_MAX];
    int        exitcode;
    int        exitsignal;
}step_register_info;
/*ע����Ϣ*/
step_register_info st_step_register_info[STEP_MAX_NUM];
/*ע����Ϣ������*/
int    step_register_i;

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
 *��ѯ���ղ�����Ϣ
 **/
int query_dayend_step_info(MYSQL *conn_ptr)
{
    /*MYSQL����*/
    MYSQL_RES   *res_ptr;
    MYSQL_ROW   res_row;
    int         row_num = -1, column_num = -1, i , j;
    char        sql_select[1024] = { 0 };
    
    for ( i = 0 ; i < STEP_MAX_NUM ; i++ )
        memset( &StepsInfo[i] , 0x00 , sizeof( stSteps ) );
    
    /*��ѯ��ִ�в�����Ϣ*/
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

    /*����������ֵ*/
    if ( row_num >= STEP_MAX_NUM )
    {
        SysLog( LOGTYPE_ERROR , "������������[%d]��ʵ��[%d]��" , STEP_MAX_NUM, row_num );
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
              SysLog( LOGTYPE_INFO , "��ѯ������[%d]����Ԥ��" , j );
              return ERROR;
            }
        }
    }
    
    return NOERR;
}

/**
 *���ݲ���ID��ѯ������Ϣ
 **/
int find_stepinfo_by_stepid( long lStepId )
{
    int i , iIdx = -1;      /*iIdx-Ŀ�경���ڲ�������е��±�*/

    for( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        if ( StepsInfo[i].lId == lStepId )
        {
            iIdx = i;
        }
    }
    
    if ( i > iDayend_Step_Num )
    {
        SysLog( LOGTYPE_ERROR , "��������в��޴˲���[%ld]" , lStepId );
        iIdx = -1;
    }
    
    return iIdx;
}



/**
 *��ѯ������ϵ
 **/
int query_dept_step_info(MYSQL *conn_ptr)
{
    /*MYSQL����*/
    MYSQL_RES   *res_ptr;
    MYSQL_ROW   res_row;
    int         row_num = -1, column_num = -1, i , j;
    char        sql_select[1024] = { 0 };
    int         iIdx;  /*����������±�*/
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
              SysLog( LOGTYPE_INFO , "��ѯ������[%d]����Ԥ��" , j );
              return ERROR;
            }
        }
        
        /*���Ʋ�����Ϣ-��������������Ϣ*/
        iIdx = find_stepinfo_by_stepid( lStepId );
        if ( iIdx >= 0 )
        {
            StepsInfo[iIdx].dayend_dept_step_list[StepsInfo[iIdx].iDepStepNum++] = dept_step_info;
        }
    }
    
    return NOERR;
}

/**
 *У����������ִ����ɷ�
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
 *���²���״̬
 *        δִ�� -> ����ִ��
 **/
int update_dayend_step_start_status( MYSQL *conn_ptr, stSteps stSigSteps )
{
    /*MYSQL����*/
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
 *���²���״̬
 *        δִ�� -> ����ִ��
 **/
int update_dayend_step_end_status( MYSQL *conn_ptr, stSteps stSigSteps , int iResult )
{
    /*MYSQL����*/
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
 *ִ�в���
 **/
void exec_step( stSteps stSigSteps )
{
    char lock_file_name[50]    = { 0 } ;
    void *handle              = NULL;        /*��̬����*/
    step_func_api func        = NULL;        /*��ں�����ַ*/
    int    iRet                = 0;
    MYSQL  conn_ptr ;
    
    /*���������ļ���*/
    sprintf( lock_file_name , "%s_%d" , LOCK_FILE_PREFIX_STEP , stSigSteps.lId );

    /*�������*/
    lock_file( lock_file_name );
    
    /*�Ǽǲ�����*/
    put_dayend_stepname( stSigSteps.sStepName );

    /*���ز���Ķ�̬��*/
    if ( NULL == ( handle = dlopen( stSigSteps.sLibName , RTLD_LAZY ) ) )
    {
        SysLog( LOGTYPE_ERROR , "�򿪲��趯̬��[%s]ʧ�� err[%s]" , 
                stSigSteps.sLibName , dlerror() );
        exit( _EXIT_FAILURE );
    }
    
    /*��ȡ����ָ��*/
    if ( NULL == ( func = ( step_func_api )dlsym( handle , stSigSteps.sFuncName ) ) )
    {
        SysLog( LOGTYPE_ERROR , "��ȡ���躯��[%s]��ڵ�ַʧ�� [%s]" , 
                stSigSteps.sFuncName , strerror( errno ) );
        dlclose( handle );
        exit( _EXIT_FAILURE );
    }

    SysLog( LOGTYPE_INFO , "����{%s]��ʼ ִ�н���[%d]" , stSigSteps.sStepName , getpid() );
    
    /*ÿһ�������������ݿ�����*/
    if ( NOERR != dbopen(&conn_ptr) )
    {
        exit( _EXIT_FAILURE );
    }
    
    /*���²���״̬����ʼ��-����ִ��*/
    if ( ( iRet = update_dayend_step_start_status( &conn_ptr, stSigSteps ) ) != NOERR )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }

    iRet = func( &conn_ptr );

    dlclose( handle );

    if ( STEP_EXEC_STATUS_SUCC != iRet && STEP_EXEC_STATUS_FAIL != iRet )
    {
        SysLog( LOGTYPE_ERROR , "ִ�н��δ֪" );
        iRet = STEP_EXEC_STATUS_UNKNOWN;
    }

    /*���²���״̬������ִ��-ִ�н��*/
    if ( ( iRet = update_dayend_step_end_status( &conn_ptr, stSigSteps , iRet ) ) != NOERR )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    SysLog( LOGTYPE_INFO , "����{%s]���" , stSigSteps.sStepName );
    
    /*�Ͽ��������ݿ�����*/
    dbclose(&conn_ptr);
    
    exit( iRet );
}


/**
 *ע����Ϣ��ʼ��
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
 *�Ǽ�ע����Ϣ��
 **/
void step_register_register ( stSteps stSigSteps , pid_t pid )
{
    st_step_register_info[step_register_i].pid = pid;
    strcpy( st_step_register_info[step_register_i].stepname , stSigSteps.sStepName );
    step_register_i++;
}


/**
 *��ѯע����Ϣ��
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
 *ע����Ϣ�ǿ�У��
 *���أ�      
 *      ERROR - �ǿգ������ȴ�
 *      NOERR - �գ�����ȴ�
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
 *�����ӦPid��ע����Ϣ
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
 *ִ�е�������
 *      stSigSteps - ����ID
 *      ch_force   - ǿ�Ƶ���ִ�б�ʶ
 **/
void exec_sig_steps( stSteps stSigSteps , int ch_force )
{
    pid_t     pid    = 0;
    
    if ( !ch_force )
    {
        /*��ѯ���������Ƿ�ִ�����*/
        if ( check_step_depend( stSigSteps ) )
        {
            return ;
        }
    }
    
    pid = fork();
    if ( pid == 0 )
    {
        /*�ӽ���ִ�в���*/

        /*����ִ�в���*/
        exec_step( stSigSteps );
    }
    else if ( pid > 0 )
    {
        /*�����̵Ǽǲ���ִ����Ϣ*/
        step_register_register( stSigSteps , pid );
    }
    else
    {
        /*�����ӽ���ִ��ʧ��*/
        SysLog( LOGTYPE_ERROR , "ִ�в���{%s]�����ӽ���ʧ��" , stSigSteps.sStepName );
    }
}


/**
 *ִ�����в���
 **/
void exec_all_steps()
{
    int i ;

    /*ִ��ÿ������*/
    for ( i = 0 ; i < iDayend_Step_Num ; i++ )
    {
        /*����˲���δ��ִ����ִ��*/
        if ( STEP_EXEC_STATUS_INIT == StepsInfo[i].iStatus )
        {
            exec_sig_steps( StepsInfo[i] , 0 );
        }
    }

}



/**
 *ִ�����в���
 **/
void wait_exec_steps()
{
    int   exec_status    = 0;        /*�ӽ���ִ��״̬*/
    pid_t  pid            = 0;
    int   iDx            = 0;        /*ע����Ϣ����Ǳ�*/
    
    while(1)
    {
        pid = wait( &exec_status );

        if ( -1 == pid )
        {
            if ( EINTR == errno )
            {
                continue;          /*���ź��ж�,�����ȴ�*/
            }
            else if ( ECHILD == errno )
            {
                break;            /*���е��ӽ��̾��ѻ���*/
            }
            else
            {
                SysLog( LOGTYPE_ERROR , "�����̵ȴ������ӽ���ʧ��[%d]-[%s]" ,
                        errno , strerror( errno ) );
            }
        }
        else
        {
            /*����PID��ѯע����Ϣ*/
            step_register_select( pid , &iDx );

            if ( WIFEXITED( exec_status ) )
            {
                /*��������(ͨ��������return��exit����)*/
                st_step_register_info[iDx].exitcode = WEXITSTATUS( exec_status );
                SysLog( LOGTYPE_INFO , "��������[%s]ִ�н��[%d]" ,
                        st_step_register_info[iDx].stepname , 
                        st_step_register_info[iDx].exitcode );
            }
            else 
            {
                if ( WIFSIGNALED( exec_status ) )
                {
                    st_step_register_info[iDx].exitsignal = WTERMSIG( exec_status );
                    SysLog( LOGTYPE_ERROR , "���ղ���[%s][ִ�н���%d]�յ��ź�[%d]�����·Ƿ�����" ,
                            st_step_register_info[iDx].stepname , 
                            pid , 
                            st_step_register_info[iDx].exitsignal );
                }
                else if ( WIFSTOPPED( exec_status ) )
                {
                    st_step_register_info[iDx].exitsignal = WSTOPSIG( exec_status );
                    SysLog( LOGTYPE_ERROR , "���ղ���[%s]���ź�[%d]ֹͣ" ,
                            st_step_register_info[iDx].stepname , 
                            st_step_register_info[iDx].exitsignal );
                }
                else if ( WIFCONTINUED( exec_status ) )
                {
                    SysLog( LOGTYPE_ERROR , "���ղ���[%s]����������ִ��" ,
                            st_step_register_info[iDx].stepname );
                }
                else
                {
                    SysLog( LOGTYPE_ERROR , "�յ����ղ���[%s]�ķǷ�״̬���֪ͨ" ,
                            st_step_register_info[iDx].stepname );
                }
            }
        }
        
        /*���ע����Ϣ*/
        step_register_clear( pid );
    }
}

void main( int argc , char** argv )
{
    int    ch = 0, iRet = NOERR;
    int    ch_force = 0, ch_step = 0;
    long   lStepId     = 0;
    int    iIdx = -1;      /*iIdx-Ŀ�경���ڲ�������е��±�*/
    
    MYSQL  conn_ptr ;
    
    /*�Ǽǲ�����*/
    put_dayend_stepname( argv[0] );
    
    while( -1 != ( ch = getopt( argc, argv, "hn:f" ) ) )
    {
        switch( ch )
        {
        case 'h':
            fprintf( stderr , "Usage: ������ -n <����ִ�в�����> -f <ǿ��ִ�У�����������ϵ> -h <help>\n" );
            exit( _EXIT_SUCCESS );
        case 'n':
            ch_step = 1;
            lStepId = atoi( optarg ) ;
            break;
        case 'f':
            ch_force = 1;
            break;
        default:
            fprintf( stderr , "Usage: ������ -n <����ִ�в�����> -f <ǿ��ִ�У�����������ϵ> -h <help>\n" );
            exit( _EXIT_FAILURE );
        }
    }
    
    /*�����ļ�������ֹ����*/
    iRet = lock_file( LOCK_FILE_DAYENDMAIN );
    if ( iRet != NOERR )
    {
        exit( _EXIT_SUCCESS );
    }
     
    /*�������������ݿ�*/
    if ( NOERR != dbopen(&conn_ptr) )
    {
        exit( _EXIT_FAILURE );
    }

    /*��ѯϵͳ��Ϣ*/
    if ( NOERR != query_sysinfo( &conn_ptr ) )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    if ( SYS_STATUS_END != get_sys_status( ) )
    {
        SysLog( LOGTYPE_INFO , "ϵͳ������״̬" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*��ѯ���ղ�����Ϣ*/
    if ( NOERR != query_dayend_step_info( &conn_ptr ) )
    {
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }

    /*��ѯ������ϵ*/
    if ( NOERR != query_dept_step_info( &conn_ptr ) )
    {
        SysLog( LOGTYPE_INFO , "before wait_step_exec" );
        dbclose(&conn_ptr);
        exit( _EXIT_FAILURE );
    }
    
    /*�Ͽ��������ݿ�����*/
    dbclose(&conn_ptr);

    /*��ʼ��ע����Ϣ*/
    step_register_init();

    if ( ch_step )
    {
        /*���ݲ���ID��ѯ������Ϣ*/
        iIdx = find_stepinfo_by_stepid( lStepId );
        
        exec_sig_steps( StepsInfo[iIdx] , ch_force );
    }
    else
    {
        exec_all_steps();
    }
    
    if ( step_register_chk() )
    {
        /*�ȴ������ӽ��̷���*/
        SysLog( LOGTYPE_INFO , "before wait_step_exec" );
        wait_exec_steps();
        SysLog( LOGTYPE_INFO , "after wait_step_exec" );
    }

    SysLog( LOGTYPE_INFO , "���ȳ����˳�..." );

    exit( iRet );
}
