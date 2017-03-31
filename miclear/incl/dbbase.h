/****************************************************************************
 * �ļ���: dbbase.h                                                        *
 * ��  ��: 2017/03/13                                                        *
 * ��  ��: MYSQL ���ݿ�(�޶���)                                             *
 ****************************************************************************/
#include "mysql.h"

//----------------------------- DB���ֶ���  ------------------------------------------------//
#define SQLNOFOUND              -100

#define PTS_DB_TYPE_INT         1
#define PTS_DB_TYPE_LONG        2
#define PTS_DB_TYPE_CHAR        3
#define PTS_DB_TYPE_DOUBLE      4

#define DB_UNKNOWN      0
#define DB_INIT         1

typedef struct
{
	long                num_rows;
	unsigned int        num_fields;
	unsigned long       * lengths ;
	MYSQL_RES           * res;
	MYSQL_ROW           row;
	MYSQL_FIELD         * mysql_fields;
}DB_RESULT;

typedef struct
{
    char * col_name;
    void * col_val ;
    char * col_desc;
    int    col_type;
    int    col_len ;
}DB_GET_ST;
