/****************************************************************************
 * 函数名: dbbase.c                                                        *
 * 描  述: 数据库底层封装 (李君凯)                                            *
 * 日  期: 2017/03/13                                                        *
 ****************************************************************************/
#include <assert.h>
#include <ctype.h>
#include <iconv.h>
#include <errno.h>

#include "dbbase.h"
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "util.h"



//select. return result rows. -1 if error.
long _db_query (MYSQL * mysql_conn, const char * sql, MYSQL_RES ** res)
{
    unsigned long num_rows = -1;

    if(!mysql_conn)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL QUERY CONN EQUAL NULL FAIL!");
        return (-1);
    }

    if (mysql_real_query (mysql_conn, sql, strlen(sql)))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL QUERY REAL QUERY [%s] FAIL: %s !", sql, mysql_error(mysql_conn));
        return (-1);
    }

    * res =  mysql_store_result (mysql_conn);
    if(( * res) == NULL)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL QUERY STORE RESULT [%s] FAIL: %s !", sql, mysql_error(mysql_conn));
        return (-1);
    }

    num_rows = (unsigned long) mysql_num_rows (* res);

    return (num_rows);
}

//insert, update, delete. return match rows num. >=0, succ; -1 if error.
long _db_exec (MYSQL * mysql_conn, const char * sql)
{
    long affect_rows = -1;

    if(!mysql_conn)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL EXEC CONN EQUAL NULL FAIL!");
        return (-1);
    }

    if (mysql_real_query (mysql_conn, sql, strlen(sql)))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL EXEC REAL QUERY [%s] FAIL: %s !", sql, mysql_error(mysql_conn));
        return (-1);
    }

    if((my_ulonglong)-1 == mysql_affected_rows(mysql_conn))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL EXEC AFFECT ROWS [%s] FAIL: %s !", sql, mysql_error(mysql_conn));
        return (-1);
    }

    affect_rows = (unsigned long) mysql_affected_rows (mysql_conn);

    return (affect_rows);
}

int _db_init (MYSQL ** _mysql_conn, const char * host_name, const char * user_name, const char * passwd, const char * db_name, unsigned int port)
{
    MYSQL * mysql_conn ;

    mysql_conn = mysql_init (NULL);
    if(!(mysql_conn))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL INIT FAIL!");
        return (-1);
    }

    if(!mysql_real_connect (mysql_conn, host_name, user_name, passwd, db_name, port, NULL, CLIENT_FOUND_ROWS))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL CONNECT FAIL: %s", mysql_error (mysql_conn));
        return (-1);
    }

    if(_db_exec (mysql_conn, "set names utf8") < 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL SET LANG UTF8 FAIL: %s", mysql_error (mysql_conn));
        return (-1);
    }

    *_mysql_conn = mysql_conn;

    return (0);
}

void _db_close (MYSQL * mysql_conn)
{
    if(mysql_conn)
    {
        mysql_close (mysql_conn);
    }

    return ;
}

//-------------------------------------------------------------------------------------------------------

int db_init (MYSQL * mysql_conn, const char * host_name, const char * user_name, const char * passwd, const char * db_name, unsigned int port)
{
    if (_db_init(&(mysql_conn), host_name, user_name, passwd, db_name, port))
        return (-1);

    return (0);
}


//insert, update, delete. return match rows num. >=0, succ; -1 if error.
long db_exec (MYSQL * mysql_conn, const char *sql)
{
    return (_db_exec (mysql_conn, sql));
}

int db_begin (MYSQL * mysql_conn)
{
    if (!mysql_conn)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].BEGIN WORK CONN FAIL!");
        return (-1);
    }

    if (db_exec (mysql_conn, "START TRANSACTION"))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].BEGIN WORK CONN FAIL: [%s]!", mysql_error(mysql_conn));
        return (-1);
    }

    return (0);
}

int db_commit (MYSQL * mysql_conn)
{
    if (!mysql_conn)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].COMMIT WORK CONN FAIL!");
        return (-1);
    }

    if (mysql_commit (mysql_conn))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].COMMIT FAIL: [%s]!", mysql_error(mysql_conn));
        return (-1);
    }

    return (0);
}

int db_rollback (MYSQL * mysql_conn)
{
    if (!mysql_conn)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].ROLLBACK CONN FAIL!");
        return (-1);
    }

    if(mysql_rollback (mysql_conn))
    {
        SysLog( LOGTYPE_ERROR, "[DBS].ROLLBACK FAIL: [%s]!", mysql_error (mysql_conn));
        return (-1);
    }

    return (0);
}

void db_close (MYSQL * mysql_conn)
{
    _db_close (mysql_conn);
    mysql_conn = NULL;
}

int db_commit_close(MYSQL * mysql_conn)
{
    int ret = -1;
    ret = db_commit (mysql_conn);
    db_close (mysql_conn);

    return (ret);
}

int db_rollback_close(MYSQL * mysql_conn)
{
    int ret = -1;
    ret = db_rollback (mysql_conn);
    db_close (mysql_conn);

    return (ret);
}

// -1, error; else return result row num;
long db_query (MYSQL * mysql_conn, DB_RESULT *db_result, const char * sql)
{
    long ret = -1;
    MYSQL_RES * res       = NULL;

    ret = _db_query (mysql_conn, sql, &res);

    db_result->res      = res;
    db_result->num_rows = ret;

    db_result->mysql_fields = mysql_fetch_fields(res);
    db_result->num_fields   = mysql_num_fields  (res);

    return (ret);
}

// -1, error; else return result row num;
long db_get_result_row_num (MYSQL * mysql_conn, DB_RESULT * db_result)
{
    if(db_result == NULL)
        return (-1);
    else
        return (db_result->num_rows);
}

// reset data seek to row 0;
void db_result_data_seek (MYSQL * mysql_conn, DB_RESULT * db_result)
{
    if(db_result && db_result->res)
        mysql_data_seek (db_result->res, 0);

    return ;
}

// -1, error; 0, null row; 1, get row data
int db_get_next_row (DB_RESULT * db_result)
{
    if(db_result == NULL)
        return (-2);

    db_result->row     = mysql_fetch_row (db_result->res);
    db_result->lengths = mysql_fetch_lengths(db_result->res);

    if(!db_result->row)
        return (0);

    return (1);
}

long db_get_last_insert_id (MYSQL * mysql_conn)
{
    long id = -1;

    id = mysql_insert_id (mysql_conn);
    if(id <= 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL INSERT ID FAIL: [%d]", mysql_error (mysql_conn), id);
    }

    return (id);
}


// -1, error; -2, name not match; 0, no more data, >0, ok
// set char/long/long long/double value to col_value by col field type
int db_get_col_val (DB_RESULT *db_result, const char * col_name, void * col_value, int col_value_len, char * val_print)
{
    int i;
    int type;
    int buff_len;

    assert(col_value_len > 0);

    memset (val_print, 0, sizeof(val_print));

    if (!col_value || strlen(col_name) == 0)
    {
        return (-1);
    }

    if (!db_result)
        return (-2);

    if (!db_result->row)
        return (0);

    for (i = 0; i < db_result->num_fields; i++)
    {
        if(strcasecmp(col_name, db_result->mysql_fields[i].name) == 0)
        {
            if(db_result->row[i] == NULL)
            {
                //if(col_value_len)
                //    *col_value_len = 0;
                // 这个地方有待改进 或者 !db_result->row 判断已经OK
                return (1);
            }

            DelSpace (db_result->row[i]);
            db_result->lengths[i] = strlen(db_result->row[i]);
            strcpy (val_print, db_result->row[i]);

            type = db_result->mysql_fields[i].type;
            switch (type)
            {
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                    if(col_value_len != sizeof(short))
                    {
                        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL GET COL FAIL: [%s SHORT!=[%d].NOT FIT]", col_name, col_value_len);
                        return (-5);
                    }
                    *((int *)col_value) = atoi(db_result->row[i]);
                    break;

                case MYSQL_TYPE_INT24:
                    if(col_value_len != sizeof(int))
                    {
                        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL GET COL FAIL: [%s INT!=[%d].NOT FIT]", col_name, col_value_len);
                        return (-5);
                    }
                    *((int *)col_value) = atoi(db_result->row[i]);
                    break;
                case MYSQL_TYPE_LONG:
                    if(col_value_len != sizeof(int))
                    {
                        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL GET COL FAIL: [%s INT!=[%d].NOT FIT]", col_name, col_value_len);
                        return (-6);
                    }
                    *((int *)col_value) = atoi(db_result->row[i]);
                    break;

                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_NEWDECIMAL: // sum()
                    if(col_value_len != sizeof(long long))
                    {
                        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL GET COL FAIL: [%s LONG!=[%d].NOT FIT]", col_name, col_value_len);
                        return (-7);
                    }
                    *((long *)col_value) = atol (db_result->row[i]);
                    break;

                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                    if(col_value_len != sizeof(double))
                    {
                        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL GET COL FAIL: [%s DOUBLE!=[%d].NOT FIT]", col_name, col_value_len);
                        return (-8);
                    }
                    *((double *)col_value) = atof(db_result->row[i]);
                    break;

                //char/varchar/date/time/datetime/...
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_TIME:
                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_YEAR:
                case MYSQL_TYPE_STRING:
                case MYSQL_TYPE_VAR_STRING:
                default:
                    if(db_result->lengths[i] > 0)
                    {
                        if(col_value_len)
                        {
                            if(col_value_len >= db_result->lengths[i] + 1)
                            {
                                strcpy (col_value, db_result->row[i]);
                            }
                            else
                            {
                                SysLog( LOGTYPE_ERROR, "[DBS].MYSQL GET COL FAIL: [%s (%d>%d|%s) LEN.NOT FIT]", col_name, db_result->lengths[i], col_value_len, db_result->row[i]);
                                return (-9);
                            }
                        }
                        else
                        {
                            strcpy (col_value, db_result->row[i]);
                        }
                    }
                    break;
            }

            return (1);
        }
    }

    SysLog( LOGTYPE_ERROR, "[DBS].MYSQL GET COL[%s] VAL FAIL: NO MATCH!", col_name);
    return (-2);
}

int db_update (MYSQL * mysql_conn, char * sql, char * _sql_info)
{
    int ret;
    char * sql_info = NULL;

    ret = -1;

    if (strlen (sql) <= 0)
        return (0);

    if (_sql_info)
        sql_info = _sql_info;
    else
        sql_info = sql;

    ret = db_exec (mysql_conn, sql);
    if (ret < 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL UPDATE FAIL: %s", sql);
        return (-1);
    }

    SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL UPDATE: %s .[%d]ROWS", sql, ret);

    return (ret);
}


int db_delete (MYSQL * mysql_conn, char * sql, char * _sql_info)
{
    int ret;
    char * sql_info = NULL;

    ret = -1;

    if (strlen (sql) <= 0)
        return (0);

    if (_sql_info)
        sql_info = _sql_info;
    else
        sql_info = sql;

    ret = db_exec (mysql_conn, sql);
    if (ret < 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL DELETE FAIL: %s", sql);
        return (-1);
    }

    SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL DELETE: %s .[%d]ROWS", sql, ret);

    return (ret);
}

int db_insert_put_val (DB_GET_ST * data, char * sql, char * _sql_info, char * title)
{
    int i ;
    char log_buffer[30960], tmp[2048], tmp1[2048], sql_insert[5120];

    memset (sql_insert, 0, sizeof(sql_insert));
    memset (log_buffer, 0, sizeof(log_buffer));
    sprintf(log_buffer, "[%s]-[%s] SUCC!\n", title, _sql_info);

    for (i=0; i<100 ;i++)
    {
        if (data[i].col_name== NULL || strlen (data[i].col_name) <= 0 || i > 100) break;

        memset (tmp , 0, sizeof(tmp));
        memset (tmp1, 0, sizeof(tmp1));

        if (i > 0)
            strcat (sql_insert, ", ");

        if (data[i].col_type == PTS_DB_TYPE_INT   )
        {
            sprintf(tmp , "          [%s] = [%d]\n", data[i].col_desc, * ((int *)data[i].col_val) );
            sprintf(tmp1, "%s='%d' ", data[i].col_desc, * ((int *)data[i].col_val) );
        }
        else
        if (data[i].col_type == PTS_DB_TYPE_LONG  )
        {
            sprintf(tmp, "          [%s] = [%ld]\n", data[i].col_desc, * ((long *)data[i].col_val) );
            sprintf(tmp1, "%s='%ld' ", data[i].col_desc, * ((long *)data[i].col_val) );
        }
        else
        if (data[i].col_type == PTS_DB_TYPE_DOUBLE)
        {
            sprintf(tmp, "          [%s] = [%.2f]\n", data[i].col_desc, * ((double *)data[i].col_val) );
            sprintf(tmp1, "%s='%f' ", data[i].col_desc, * ((double *)data[i].col_val) );
        }
        else
        {
            sprintf(tmp, "          [%s] = [%s]\n"  , data[i].col_desc, data[i].col_val);
            sprintf(tmp1, "%s='%s' ", data[i].col_desc, data[i].col_val );
        }

        strcat (log_buffer, tmp);
        strcat (sql_insert, tmp1);
    }

    strcat (sql, sql_insert);
    SysLog( LOGTYPE_DEBUG, "%s", log_buffer);

    return (0);
}

int db_insert_one (DB_GET_ST * data, MYSQL * mysql_conn, char * sql, char * _sql_info, char * title)
{
    int ret = -1;
    char * sql_info = NULL;

    if (strlen (sql) <= 0)
        return (0);

    if (_sql_info)
        sql_info = _sql_info;
    else
        sql_info = sql;

    db_insert_put_val (data, sql, _sql_info, title);

    ret = db_exec (mysql_conn, sql);
    if (ret < 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL INSERT FAIL: %s", sql);
        return (-1);
    }

    SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL INSERT: %s .[%d]ROWS", sql, ret);

    return (ret);
}

int db_select_get_val (DB_GET_ST * data, char * _sql_info, DB_RESULT * db_result, char * title)
{
    int i ;
    int i_val;
    long l_val;
    double d_val;
    char debug_buffer[2048];
    char log_buffer[30960], tmp[2048], buf[2048];

    memset (log_buffer, 0, sizeof(log_buffer));
    sprintf(log_buffer, "[%s]-[%s] SUCC!\n", title, _sql_info);

    for (i=0; i<100 ;i++)
    {
        if (data[i].col_name== NULL || strlen (data[i].col_name) <= 0 || i > 100) break;

        memset (debug_buffer, 0, sizeof(debug_buffer));
        db_get_col_val (db_result, data[i].col_name, data[i].col_val, data[i].col_len, debug_buffer);

        memset (tmp, 0, sizeof(tmp));

        if (data[i].col_type == PTS_DB_TYPE_INT   )
        {
            i_val = * ((int *)data[i].col_val) ;
            sprintf(tmp, "          [%s] = [%d]\n", data[i].col_desc, i_val );
        }
        else
        if (data[i].col_type == PTS_DB_TYPE_LONG  )
        {
            l_val = * ((long *)data[i].col_val) ;
            sprintf(tmp, "          [%s] = [%ld]\n", data[i].col_desc, l_val );
        }
        else
        if (data[i].col_type == PTS_DB_TYPE_DOUBLE)
        {
            d_val = * ((int *)data[i].col_val) ;
            sprintf(tmp, "          [%s] = [%.2f]\n", data[i].col_desc, d_val );
        }
        else
            sprintf(tmp, "          [%s] = [%s]\n"  , data[i].col_desc, data[i].col_val);

        strcat (log_buffer, tmp);
    }

    SysLog( LOGTYPE_DEBUG, "%s", log_buffer);

    return (0);
}

/*释放数据集*/
void db_free_result(DB_RESULT * db_result)
{
    mysql_free_result(db_result->res);
    
    free(db_result);
}

/**
 *data：       结构体数组容器
 *mysql_conn： 数据库连接描述符
 *db_result    查询结果集信息
 *sql          sql
 *_sql_info    sql标题
 *title        程序标题
 **/
int db_select_one (DB_GET_ST * data, MYSQL * mysql_conn, char * sql, char * _sql_info, char * title)
{
    int ret = -1;
    char * sql_info = NULL;
    DB_RESULT * db_result = NULL;
        
    if (strlen (sql) <= 0)
        return (0);

    if (_sql_info)
        sql_info = _sql_info;
    else
        sql_info = sql;
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    ret = db_query (mysql_conn, db_result, sql);
    if (ret < 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL SELECT FAIL: %s", sql);
        db_free_result(db_result);
        return (-1);
    }
    else
    if (ret == 0)
    {
        db_free_result(db_result);
        return (SQLNOFOUND);
    }

    SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL SELECT: %s .[%d]ROWS", sql, ret);

    ret = db_get_next_row (db_result);
    if(ret != 1)
    {
        SysLog( LOGTYPE_ERROR, "%s GET ROW .FAIL", sql);
        db_free_result(db_result);
        return (-2);
    }

    db_select_get_val (data, sql_info, db_result, title);
    
    db_free_result(db_result);
    
    return (0);
}

int db_exsit_check (MYSQL * mysql_conn, char * sql, char * _sql_info, char * title)
{
    int ret = -1;
    char * sql_info = NULL;
    DB_RESULT * db_result = NULL;
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    if (strlen (sql) <= 0)
        return (0);

    if (_sql_info)
        sql_info = _sql_info;
    else
        sql_info = sql;

    ret = db_query (mysql_conn, db_result, sql);
    if (ret < 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL SELECT FAIL: %s", sql);
        return (-1);
    }
    else
    if (ret == 0)
    {
        SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL SELECT NO DATA: %s", sql);
        return (SQLNOFOUND);
    }
    else
    {
        SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL SELECT: %s .[%d]ROWS", sql, ret);
        return (ret);
    }
}

/**
 *mysql_conn： 数据库连接描述符
 *db_result    查询结果集信息
 *_sql_info    sql标题
 *title        程序标题
 **/
int db_exec_cursor (MYSQL * mysql_conn, DB_RESULT * db_result, char * sql, char * _sql_info, char * title)
{
    int ret = -1;
    char * sql_info = NULL;
    
    if (strlen (sql) <= 0)
        return (0);

    if (_sql_info)
        sql_info = _sql_info;
    else
        sql_info = sql;

    ret = db_query (mysql_conn, db_result, sql);
    if (ret < 0)
    {
        SysLog( LOGTYPE_ERROR, "[DBS].MYSQL SELECT FAIL: %s", sql);
        return (-1);
    }
    else
    if (ret == 0)
    {
        SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL SELECT: %s .[%d]ROWS", sql, ret);
        return (SQLNOFOUND);
    }

    SysLog( LOGTYPE_DEBUG, "[DBS].MYSQL SELECT: %s .[%d]ROWS", sql, ret);
    
    return (0);
}

/**
 *data：       结构体数组容器
 *db_result    查询结果集信息
 *_sql_info    sql标题
 *title        程序标题
 **/
int db_fetch_cursor(DB_GET_ST * data, DB_RESULT * db_result, char * _sql_info, char * title)
{
    int ret;
    ret = db_get_next_row (db_result);
    if(ret != 1)
    {
        SysLog( LOGTYPE_ERROR, "%s GET ROW .FAIL", _sql_info);
        return (-2);
    }

    db_select_get_val (data, _sql_info, db_result, title);
    
    return ret;
}