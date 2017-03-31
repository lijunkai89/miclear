/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 数据库公共函数
 *    SystemName  : 清分清算系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-数据库公共函数
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/26         北京           李君凯         创建文档
******************************************************************************/
#include <string.h>

#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "util.h"
#include "dbbase.h"
#include "mysql.h"

/**
 *连接数据库
 **/
int _dbopen( MYSQL *conn_ptr, char *_host, char *_username , char *_password , char *_db_str )
{
    conn_ptr = mysql_init(conn_ptr);
    if ( !conn_ptr )
    {
        printf("mysql_init failed\n");
        return ERROR;
    }

    conn_ptr = mysql_real_connect(conn_ptr, _host, _username, _password, _db_str, 0, NULL, 0);
    if ( conn_ptr )
    {
        SysLog( LOGTYPE_INFO , "connect (%s/%s@%s) success" , 
                    _username , _password , _db_str );
        
        /*设置编码*/
        mysql_set_character_set( conn_ptr, "GBK" );
        //mysql_options(conn_ptr, MYSQL_SET_CHARSET_NAME, "utf8");
        //mysql_real_query(conn_ptr, "SET NAMES GBK;", (unsigned long) strlen ("SET NAMES GBK;"));
    }
    else
    {
        SysLog( LOGTYPE_ERROR , "connect (%s/%s@%s) error [%d]-[%s]" , 
                    _username , _password , _db_str , mysql_errno(conn_ptr) , mysql_error(conn_ptr) );
        return ERROR;
    }

    return NOERR;

}

/**
 *从文件中读取数据库用户信息
 *flag 0-未找到关键字头
 *     1-找到关键字头，但未找到关键字
 *     2-取得关键字对应的值
 **/
int gen_db_info( char *sFileName , char *sKeyWordHead , char *sKeyWord , char *sValues )
{
    FILE *fp;
    char buf[400],tmpbuf[400];
    char _sKeyWordHead[400];
    int flag = 0;
    char *ptb , *psend ;
    int len , Size ;

    if ( !( fp = fopen(sFileName , "r") ) ) 
    {
        printf( " open sFileName=[%s] error",sFileName ) ;
        return 1 ;
    }

    sprintf( _sKeyWordHead ,"[%s]",sKeyWordHead );

    while ( !feof( fp ) ) 
    {
        memset( buf ,0x0 ,sizeof(buf ));
        fgets(buf,sizeof(buf),fp);
        if ( buf[ 0 ] == '#' )
        {
            continue ;
        }
        else if ( memcmp(buf ,_sKeyWordHead ,strlen(_sKeyWordHead)) == 0 ) 
        {
            flag = 1;  /*找到头关键字标识 0-未找到 1-找到*/
            continue;
        }
        else
        {
            if ( flag == 1 ) 
            {
                if (memcmp( buf  ,sKeyWord ,strlen(sKeyWord) ) == 0 ) 
                {
                    memset( tmpbuf ,0x0 ,sizeof(tmpbuf));
                    memcpy( tmpbuf ,&buf[strlen(sKeyWord)] ,strlen(buf) - strlen(sKeyWord) );
                    len = strlen ( tmpbuf );
                    if ( tmpbuf[len-1] == '\n' )
                        tmpbuf[len-1 ] = 0x0 ;
                    len = strlen ( tmpbuf );

                    ptb = tmpbuf ;
                    while ( *ptb == ' ' || * ptb == '=' )
                        ptb ++ ;
                    if ( ptb - tmpbuf >= strlen(tmpbuf) )
                        break;

                    Size = len -( ptb - tmpbuf ) ;

                    memcpy( sValues ,ptb ,Size );
                    flag = 2;
                    break;
                }
                continue;
            }
          }
    }

    fclose(fp);
    if ( flag == 1 || flag == 0 )
        return 1 ;
    else
        return 0;
}



int _func_get_db_info( char *db_usr , char *db_password )
{
    char username[128];
    char password[128];
    char confile[128];
    int  iRet = -1;
    char buf[128];

    memset(username,0x00,sizeof(username));
    memset(password,0x00,sizeof(password));
    memset(confile,0x00,sizeof(confile));

    sprintf(confile,"%s/etc/dbset.ini",getenv("HOME"));

    iRet = gen_db_info(confile,"CONFIG","username", username );
    if ( iRet < 0 ) 
    {
        return -1;
    }

    iRet = gen_db_info(confile,"CONFIG","password", password );
    if ( iRet < 0 ) 
    {
        return -1;
    }

    iRet = DataDES(softKey, username, buf, '1');
    if ( iRet != 0 ) 
    {
        return -1;
    }
    Asc2Hex(buf, db_usr, 32);

    iRet = DataDES(softKey, password, buf, '1');
    if ( iRet != 0 ) 
    {
        return -1;
    }
    Asc2Hex(buf, db_password, 32);

}


/**
 *连接数据库
 **/
int dbopen(MYSQL *conn_ptr)
{
    char  db_name[128];
    char  db_usr[128];
    char  db_password[128];
    char  db_host[128];
    
    memset(db_name,0x00,sizeof(db_name));
    memset(db_usr,0x00,sizeof(db_usr));
    memset(db_password,0x00,sizeof(db_password));
    memset(db_host,0x00,sizeof(db_host));
    
    _func_get_db_info( db_usr , db_password );

    strcpy(db_host,getenv( "MYSQL_HOST" ));
    strcpy(db_name,getenv( "MYSQL_NAME" ));
    
    return _dbopen(conn_ptr, db_host, db_usr , db_password , db_name );
}
/**
 *断开数据库
 *调度程序使用
 **/
void dbclose(MYSQL *conn_ptr)
{
    mysql_close(conn_ptr);

    SysLog( LOGTYPE_INFO , "disconnect database success" );
    
    return;
}


/**
 *功能说明：
 *        无返回行数的sql执行函数
 *参数说明：
 *        conn_ptr 打开的mysql连接指针
 *        sql_exec 拼接的sql字符串
 *适用对象：
 *        调度使用
 **/
int MYSQL_EXEC( MYSQL *conn_ptr, char *sql_exec )
{
    int flag;
    
    SysLog( LOGTYPE_INFO , "sql_exec = [%s]", sql_exec );
    
    flag = mysql_query(conn_ptr, sql_exec);
    if ( flag )
    {
        SysLog( LOGTYPE_INFO , "exec sql failed [%d]-[%s]" , mysql_errno(conn_ptr) , mysql_error(conn_ptr) );
        return ERROR;
    }
    else
    {
        SysLog( LOGTYPE_INFO , "exec sql sucess! affect [%d] rows", mysql_affected_rows(conn_ptr) );
        return NOERR;
    }
    
    return NOERR;
}


/**
 *sequence
 **/
int db_get_next_seq( MYSQL *conn_ptr, char *seq_name, char *seq )
{
    int         iRet = -1;
    char        sql_exec[4096] = { 0 };
    char        sql_info[512] = { 0 };
    /*seq参数*/
    long        seq_cur_val = 0;
    long        seq_min_val = 0;
    long        seq_max_val = 0;
    int         seq_increment_step = 0;;
    long        seq_next_val = 0;
    
    DB_GET_ST data [] = {
        "f_curr_val"         , &seq_cur_val    , "当前序列", PTS_DB_TYPE_LONG , sizeof(seq_cur_val ) ,
        "f_min_val"          , &seq_min_val    , "最小序列值", PTS_DB_TYPE_LONG , sizeof(seq_min_val) ,
        "f_max_val"          , &seq_max_val    , "最大序列值", PTS_DB_TYPE_LONG , sizeof(seq_max_val ) ,
        "f_increment_step"   , &seq_increment_step   , "步长", PTS_DB_TYPE_INT , sizeof(seq_increment_step) ,
        ""               , NULL             , NULL          , 0               , 0};
        
    /*开启事务*/
    iRet = db_begin(conn_ptr);
    if ( NOERR != iRet )
    {
        mysql_rollback(conn_ptr);
        return NOERR;
    }
    
    /*取当前序列值*/
    strcpy (sql_info   , "SELECT T_SEQ");
    sprintf( sql_exec,
            " select t.f_curr_val, "
            "        t.f_min_val, "
            "        t.f_max_val, "
            "        t.f_increment_step "
            "   from t_seq "
            "t where t.f_seq_name = \'%s\' for update "
            , seq_name
    );
    iRet = db_select_one (data, conn_ptr, sql_exec, sql_info, "获取序列");
    if ( iRet < 0)
    {
        db_rollback (conn_ptr);
        return (iRet);
    }

    /*生成新序列值*/
    if ( seq_cur_val + seq_increment_step > seq_max_val )
    {
        seq_next_val = seq_min_val;
    }
    else
    {
        seq_next_val = seq_cur_val + seq_increment_step;
    }
    sprintf( seq , "%ld", seq_next_val );
    
    /*更新当前序列之*/
    memset( sql_info, 0x00, sizeof( sql_info ) );
    memset( sql_exec, 0x00, sizeof( sql_exec ) );
    strcpy (sql_info    , "UPDATE T_SEQ");
    sprintf( sql_exec,
            "update t_seq t "
            "   set t.f_curr_val = %ld "
            " where t.f_seq_name = \'%s\'"
            , seq_next_val
            , seq_name
    );
    iRet = db_update( conn_ptr , sql_exec, "更新序列" );
    if ( iRet <= 0 )
    {
        db_rollback (conn_ptr);
        return ERROR;
    }
    
    /*提交事务*/
    iRet = db_commit (conn_ptr);
    if ( iRet < 0)
    {
        db_rollback (conn_ptr);
        return (-1);
    }
}

/*取出机构编码*/
int db_get_acq_list(MYSQL *conn_ptr, char acq_list_arr[][12])
{
    char sql_exec[4096]  = { 0 };
    MYSQL_RES   *res_ptr;
    MYSQL_ROW   res_row;
    int         row_num = -1, i = -1;
    
    sprintf( sql_exec,
            "select f_acq_code from t_acquirer_code_list t where t.f_status = 1 and t.f_acq_code <> ''"
    );
    
    if ( NOERR != MYSQL_EXEC( conn_ptr , sql_exec ) )
    {
        return ERROR;
    }
    res_ptr = mysql_store_result( conn_ptr );
    /*取机构号*/
    row_num = mysql_num_rows( res_ptr );

    for ( i = 0; i < row_num; i++ )
    {
        res_row = mysql_fetch_row( res_ptr );
        strcpy( acq_list_arr[i], res_row[0] );
    }

    return NOERR;
}

/**
 *获得流水id
 **/
int gen_dtl_id( MYSQL *conn_ptr, char *seq_name, char *sPk_id )
{
    char sequence[21]     = { 0 };
    int  i;
    char  szDailyDate[9]    = { 0 };
    
    get_dayend_date( szDailyDate );
    
    db_get_next_seq(conn_ptr, seq_name, sequence);
    
    sprintf( sPk_id , "%s%010s", szDailyDate, sequence );
    
    /*左补0*/
    for( i = 0; i < strlen( sPk_id ) ; i++ )
    {
       if ( sPk_id[i] == ' ' )
           sPk_id[i] = '0';
    }
        
    return NOERR;
}