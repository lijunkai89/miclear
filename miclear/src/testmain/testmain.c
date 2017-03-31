#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

void main()
{
    char sql_exec[1024] = { 0 };
    char sql_select[1024] = { 0 };
    MYSQL_RES *res_ptr;
    MYSQL conn_ptr ;
    MYSQL_ROW res_row;
    MYSQL_FIELD *field ;
    int row_num = -1,column_num = -1;
    int i , j;
    
    memset( &conn_ptr , 0x00 , sizeof( MYSQL ) );
    sprintf( sql_select , "%s" , "select * from t_sys_daily_info " );
    
    dbopen( &conn_ptr );
    
    
    #if 0
    if ( mysql_query( &conn_ptr , sql_select ) )
    {
        DayEndLog( LOGTYPE_INFO , "query failed [%d]-[%s]" , mysql_errno(&conn_ptr) , mysql_error(&conn_ptr) );
        exit( -1 );
    }
    #endif
    MYSQL_EXEC( &conn_ptr , sql_select );
    res_ptr = mysql_store_result( &conn_ptr );
    row_num = mysql_num_rows( res_ptr );
    column_num = mysql_num_fields( res_ptr );
    for ( i = 0 ; i < row_num ; i++ )
    {
        res_row = mysql_fetch_row( res_ptr );
        for ( j = 0 ; j < column_num ; j++ )
        {
            field = mysql_fetch_field( res_ptr );
            DayEndLog( LOGTYPE_INFO , "[%s]",res_row[j] );
        }
    }
    for ( i = 0 ; field = mysql_fetch_field( res_ptr ) ; i++ )
    {
        DayEndLog( LOGTYPE_INFO , "[%s]" , field->name );
    }
    
    sprintf( sql_exec , "%s" , "delete from t_sys_daily_info " );
    MYSQL_EXEC_ROW( &conn_ptr , sql_exec, &row_num ) ;
    
    MYSQL_EXEC( &conn_ptr , sql_select );
    res_ptr = mysql_store_result( &conn_ptr );
    row_num = mysql_num_rows( res_ptr );
    column_num = mysql_num_fields( res_ptr );
    for ( i = 0 ; i < row_num ; i++ )
    {
        res_row = mysql_fetch_row( res_ptr );
        for ( j = 0 ; j < column_num ; j++ )
        {
            field = mysql_fetch_field( res_ptr );
            DayEndLog( LOGTYPE_INFO , "[%s]",res_row[j] );
        }
    }
    for ( i = 0 ; field = mysql_fetch_field( res_ptr ) ; i++ )
    {
        DayEndLog( LOGTYPE_INFO , "[%s]" , field->name );
    }
    dbclose( &conn_ptr );

}
