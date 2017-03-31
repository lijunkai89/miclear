/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������ϵͳ
 *    SystemName  : ���Ȳ���-��������
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-��������
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/11         ����           �����         �����ĵ�(���޸Ķ�������)
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "dbbase.h"
#include "mysql.h"

/*������������*/
#define MAX_DEFF_DAYS 2
#define LOG_SQL_TITLE "��������"

static char  szDailyDate[9]    = { 0 };

typedef struct
{
    long    f_id            ;
    char    f_trace_no[6+1]        ;
    char    f_tran_time[10+1]      ;
    char    f_card_no[19+1]        ;
    long    f_tran_amt             ;
    int     f_tran_type            ;
    char    f_mcc[4+1]             ;
    char    f_merchant_no[15+1]    ;
    char    f_terminal_no[8+1]     ;
    char    f_tran_rrn[12+1]       ;
    char    f_auth_no[6+1]         ;
    char    f_resp_code[2+1]       ;
    char    f_check_flag[1+1]      ;
    char    f_check_date[8+1]      ;
    char    f_comments[255+1]      ;
}T_LIQUIDATE_ERR_ST;

typedef struct
{
    long    f_id                     ;
    char    f_gen_dt[19+1]           ;
    char    f_file_date[8+1]         ;
    char    f_file_name[50+1]        ;
    int     f_channel_no       ;
    char    f_load_time[19+1]        ;
    int     f_file_cnt               ;
    long    f_file_amt               ;
    int     f_status                 ;
    char    f_check_begin_time[19+1] ;
    char    f_check_end_time[19+1]   ;
    int     f_sys_cnt                ;
    long    f_sys_amt                ;
    int     f_check_bal_cnt          ;
    long    f_check_bal_amt          ;
    int     f_err_cnt                ;
    long    f_err_amt                ;
    long    f_total_fee              ;
    char    f_comments[255+1]        ;
}T_LIQUIDATE_FILE_ST;

typedef struct
{
    long    f_id                   ;
    int     f_channel_no     ;
    char    f_agent_code[11+1]     ;
    char    f_snd_code[11+1]       ;
    char    f_rcv_code[11+1]       ;
    char    f_iss_code[11+1]       ;
    char    f_trace_no[6+1]        ;
    char    f_tran_time[10+1]      ;
    char    f_card_no[19+1]        ;
    long    f_tran_amt             ;
    int     f_tran_type            ;
    char    f_mcc[4+1]             ;
    char    f_merchant_no[15+1]    ;
    char    f_terminal_no[8+1]     ;
    char    f_tran_rrn[12+1]       ;
    char    f_auth_no[6+1]         ;
    char    f_msg_code[4+1]        ;
    char    f_tran_code[6+1]       ;
    char    f_sub_tran_code[2+1]   ;
    char    f_resp_code[2+1]       ;
    long    f_rcv_fee              ;
    long    f_pay_fee              ;
    long    f_tran_fee             ;
    char    f_org_trace_no[6+1]    ;
    char    f_org_tran_time[10+1]  ;
    char    f_data_acom[1024+1]    ;
    char    f_check_flag[1+1]      ;
    char    f_check_date[8+1]      ;
    char    f_comments[255+1]      ;
    char    f_file_date[8+1]       ;
}T_LIQUIDATE_FILE_DTL_ST;

typedef struct
{
    int     ID                     ;
    char    ORDER_ID[32+1]         ;
    char    TXN_TIME[14+1]         ;
    char    CUSTOMER_NM[50+1]      ;
    char    BANK_NUM[19+1]         ;
    char    TXN_AMT[20+1]          ;
    char    ACC_TYPE[2+1]          ;
    char    CHECK_FLAG[1+1]        ;
    char    CHECK_COMMENT[2000+1]  ;
    char    TRAN_FLAG[10+1]        ;
    char    ERROR_CODE[30+1]       ;
    char    ERROR_MSG[2000+1]      ;
    char    CREATE_BY[64+1]        ;
    char    CREATE_DATE[30+1]      ;
    char    UPDATE_BY[64+1]        ;
    char    UPDATE_DATE[30+1]      ;
    long    SHBILL_ID              ;
    char    SHOP_NO[15+1]          ;
    char    SHOP_NAME[80+1]        ;
    int     SETTLE_DATE            ;
    long    ACCT_TIME              ;
    char    SHOP_MAP_ID[8+1]       ;
    char    TRACE_NO[6+1]          ;
    char    TRACE_TIME[10+1]       ;
    char    CHECK_DATE[8+1]        ;
}DF_DATA_ST;

typedef struct {
    T_LIQUIDATE_ERR_ST         t_liquidate_err_st;
    T_LIQUIDATE_FILE_ST        t_liquidate_file_st;
    T_LIQUIDATE_FILE_DTL_ST    t_liquidate_file_dtl_st;
    DF_DATA_ST                 df_data_st;
    char                       f_trace_no[6+1];
    char                       f_tran_time[10+1];
}CHK_UNION_ST;

int db_prepare_err_info(MYSQL *conn_ptr, CHK_UNION_ST *chk_union_st, char *check_flag)
{
    char  sPk_id[21]         = { 0 };    /*��ˮ�ļ���������*/
    int   iRet               = -1;
    
    /*��ȡ��ˮid*/
    iRet = gen_dtl_id(conn_ptr, "seq_liquidate_err_id", sPk_id);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "��ȡ��ˮidʧ�ܣ�" );
        return ERROR;
    }
    
    chk_union_st->t_liquidate_err_st.f_id = atol(sPk_id);
    
    if ( strcmp( check_flag, FUNDCHNL_CHK_STATUS_LESS ) == 0 )
    {
        strcpy( chk_union_st->t_liquidate_err_st.f_trace_no,    chk_union_st->t_liquidate_file_dtl_st.f_trace_no );
        strcpy( chk_union_st->t_liquidate_err_st.f_tran_time,   chk_union_st->t_liquidate_file_dtl_st.f_tran_time );
        strcpy( chk_union_st->t_liquidate_err_st.f_card_no,     chk_union_st->t_liquidate_file_dtl_st.f_card_no );
        chk_union_st->t_liquidate_err_st.f_tran_amt           = chk_union_st->t_liquidate_file_dtl_st.f_tran_amt;
        chk_union_st->t_liquidate_err_st.f_tran_type          = chk_union_st->t_liquidate_file_dtl_st.f_tran_type;
        strcpy( chk_union_st->t_liquidate_err_st.f_mcc,         chk_union_st->t_liquidate_file_dtl_st.f_mcc );
        strcpy( chk_union_st->t_liquidate_err_st.f_merchant_no, chk_union_st->t_liquidate_file_dtl_st.f_merchant_no );
        strcpy( chk_union_st->t_liquidate_err_st.f_terminal_no, chk_union_st->t_liquidate_file_dtl_st.f_terminal_no );
        strcpy( chk_union_st->t_liquidate_err_st.f_tran_rrn,    chk_union_st->t_liquidate_file_dtl_st.f_tran_rrn );
        strcpy( chk_union_st->t_liquidate_err_st.f_auth_no,     chk_union_st->t_liquidate_file_dtl_st.f_auth_no );
        strcpy( chk_union_st->t_liquidate_err_st.f_resp_code,   chk_union_st->t_liquidate_file_dtl_st.f_resp_code );
        strcpy( chk_union_st->t_liquidate_err_st.f_check_flag,  FUNDCHNL_CHK_STATUS_LESS );
        strcpy( chk_union_st->t_liquidate_err_st.f_check_date,  chk_union_st->t_liquidate_file_dtl_st.f_check_date );
    }
    else
    {
        strcpy( chk_union_st->t_liquidate_err_st.f_trace_no,    chk_union_st->df_data_st.TRACE_NO );
        strcpy( chk_union_st->t_liquidate_err_st.f_tran_time,   chk_union_st->df_data_st.TRACE_TIME );
        strcpy( chk_union_st->t_liquidate_err_st.f_card_no,     chk_union_st->df_data_st.BANK_NUM );
        chk_union_st->t_liquidate_err_st.f_tran_amt           = atol(chk_union_st->df_data_st.TXN_AMT);
        strcpy( chk_union_st->t_liquidate_err_st.f_merchant_no, chk_union_st->df_data_st.SHOP_NO );
        strcpy( chk_union_st->t_liquidate_err_st.f_check_flag,  FUNDCHNL_CHK_STATUS_MORE );
        strcpy( chk_union_st->t_liquidate_err_st.f_check_date,  chk_union_st->df_data_st.CHECK_DATE );
    }
    
    return NOERR;
}

/**
 *�Ǽǲ��
 **/
int check_err(MYSQL *conn_ptr, CHK_UNION_ST *chk_union_st)
{
    int ret ;
    char sql[8024], sql_info[512];
    DB_GET_ST data [] = {
        "f_id"                ,    &chk_union_st->t_liquidate_err_st.f_id             ,  "f_id"                , PTS_DB_TYPE_LONG,   sizeof(chk_union_st->t_liquidate_err_st.f_id            ),
        "f_trace_no"          ,     chk_union_st->t_liquidate_err_st.f_trace_no       ,  "f_trace_no"          , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_trace_no            ),
        "f_tran_time"         ,     chk_union_st->t_liquidate_err_st.f_tran_time      ,  "f_tran_time"         , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_tran_time        ),
        "f_card_no"           ,     chk_union_st->t_liquidate_err_st.f_card_no        ,  "f_card_no"           , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_card_no        ),
        "f_tran_amt"          ,    &chk_union_st->t_liquidate_err_st.f_tran_amt       ,  "f_tran_amt"          , PTS_DB_TYPE_LONG,   sizeof(chk_union_st->t_liquidate_err_st.f_tran_amt        ),
        "f_tran_type"         ,    &chk_union_st->t_liquidate_err_st.f_tran_type      ,  "f_tran_type"         , PTS_DB_TYPE_INT ,   sizeof(chk_union_st->t_liquidate_err_st.f_tran_type              ),
        "f_mcc"               ,     chk_union_st->t_liquidate_err_st.f_mcc            ,  "f_mcc"               , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_mcc        ),
        "f_merchant_no"       ,     chk_union_st->t_liquidate_err_st.f_merchant_no    ,  "f_merchant_no"       , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_merchant_no    ),
        "f_terminal_no"       ,     chk_union_st->t_liquidate_err_st.f_terminal_no    ,  "f_terminal_no"       , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_terminal_no                ),
        "f_tran_rrn"          ,     chk_union_st->t_liquidate_err_st.f_tran_rrn       ,  "f_tran_rrn"          , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_tran_rrn            ),
        "f_auth_no"           ,     chk_union_st->t_liquidate_err_st.f_auth_no        ,  "f_auth_no"           , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_auth_no            ),
        "f_resp_code"         ,     chk_union_st->t_liquidate_err_st.f_resp_code      ,  "f_resp_code"         , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_resp_code            ),
        "f_check_flag"        ,     chk_union_st->t_liquidate_err_st.f_check_flag     ,  "f_check_flag"        , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_check_flag            ),
        "f_check_date"        ,     chk_union_st->t_liquidate_err_st.f_check_date     ,  "f_check_date"        , PTS_DB_TYPE_CHAR,   sizeof(chk_union_st->t_liquidate_err_st.f_check_date            ),
        ""                    ,     NULL                                     ,  NULL               , 0               ,   0 };

    memset (sql        , 0, sizeof(sql));
    strcpy (sql_info   , "INSERT T_LIQUIDATE_ERR");
    sprintf(sql        , "INSERT INTO T_LIQUIDATE_ERR SET ");

    ret = db_insert_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (ret);
    }
    
    return NOERR;
}

/**
 *��ʼ������
 **/
int check_init(MYSQL *conn_ptr)
{
    char sql_exec[1024] = { 0 };
    int   iRet               = -1;
    
    /*��ʼ�������ļ���ˮ���˱�ʶ*/
    sprintf( sql_exec,
            " update t_liquidate_file_dtl t "
            " set t.f_check_flag = \'%s\', "
            " t.f_check_date = \'\' "
            " where t.f_check_date = \'%s\' "
            " and t.f_channel_no = %d "
            , FUNDCHNL_CHK_STATUS_INIT
            , szDailyDate
            , CHANNEL_CUP_DAIFU
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*��ʼ��������ˮ��*/
    memset( sql_exec , 0x00 , sizeof( sql_exec ) );
    sprintf( sql_exec,
            " update df_data t "
            " set t.check_flag = \'%s\', "
            " t.check_date = \'\' "
            " where t.check_date = \'%s\' "
            , FUNDCHNL_CHK_STATUS_INIT
            , szDailyDate
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*��ʼ������ͳ������*/
    memset( sql_exec , 0x00 , sizeof( sql_exec ) );
    sprintf( sql_exec,
            " update t_liquidate_file t "
            " set t.F_status = %d, "
            " t.f_check_begin_time = date_format(sysdate(),'%%Y-%%m-%%d %%H:%%i:%%s'), "
            " t.f_check_end_time = \'\', "
            " t.f_sys_cnt = 0, "
            " t.f_sys_amt = 0, "
            " t.f_check_bal_cnt = 0, "
            " t.f_check_bal_amt = 0, "
            " t.f_err_cnt = 0, "
            " t.f_err_amt = 0 "
            " where t.f_file_date = \'%s\' "
            " and t.f_channel_no = %d "
            , LIQUIDATE_FILE_STATUS_LOAD_SUCC
            , szDailyDate
            , CHANNEL_CUP_DAIFU
    );
    iRet = db_update( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ɾ�����ղ����Ĳ��*/
    memset( sql_exec , 0x00 , sizeof( sql_exec ) );
    sprintf( sql_exec,
            " delete from t_liquidate_err "
            " where f_check_date = \'%s\' "
            , szDailyDate
    );
    iRet = db_delete( conn_ptr , sql_exec, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    return NOERR;
}

/**
 *ƥ�䲢����ϵͳ����ˮ��
 **/
int db_update_df_data(MYSQL *conn_ptr, CHK_UNION_ST *chk_union_st, char *check_flag)
{
    int iRet ;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };

    memset (sql         , 0, sizeof(sql));
    strcpy (sql_info    , "UPDATE DF_DATA");
    sprintf(sql, 
           "update df_data t "
           "   set t.check_flag = \'%s\', "
           "       t.check_date = \'%s\' "
           " where t.trace_no = \'%s\' "
           "   and t.trace_time = \'%s\' "
           "   and t.check_flag = \'%s\' "
           , check_flag
           , szDailyDate
           , chk_union_st->f_trace_no
           , chk_union_st->f_tran_time
           , FUNDCHNL_CHK_STATUS_INIT
    );

    iRet = db_update (conn_ptr, sql, sql_info);
    if (iRet < 0)
    {
        return (-1);
    }

    return (iRet);
}

/**
 *�����ļ���ˮ���˽��
 **/
int db_update_file_dtl(MYSQL *conn_ptr, CHK_UNION_ST *chk_union_st, char *check_flag)
{
    int iRet ;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };

    memset (sql         , 0, sizeof(sql));
    strcpy (sql_info    , "UPDATE T_LIQUIDATE_FILE_DTL");
    sprintf(sql, 
           "update t_liquidate_file_dtl t "
           "   set t.f_check_flag = \'%s\', "
           "       t.f_check_date = \'%s\' "
           " where t.f_trace_no = \'%s\' "
           "   and t.f_tran_time = \'%s\' "
           "   and t.f_check_flag = \'%s\' "
           , check_flag
           , szDailyDate
           , chk_union_st->f_trace_no
           , chk_union_st->f_tran_time
           , FUNDCHNL_CHK_STATUS_INIT
    );

    iRet = db_update (conn_ptr, sql, sql_info);
    if (iRet < 0)
    {
        return (-1);
    }

    return (iRet);
}

/**
 *ȡ���Ǽ���ˮ������
 **/
int db_get_file_dtl_info(DB_RESULT * db_result, CHK_UNION_ST *chk_union_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "f_id"       , &chk_union_st->t_liquidate_file_dtl_st.f_id       , "ҵ��id",   PTS_DB_TYPE_LONG ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_id    ),
        "f_channel_no"       , &chk_union_st->t_liquidate_file_dtl_st.f_channel_no       , "ͨ�����" ,   PTS_DB_TYPE_INT  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_channel_no    ),
        "f_agent_code"       , chk_union_st->t_liquidate_file_dtl_st.f_agent_code       , "����������루32��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_agent_code    ),
        "f_snd_code"       , chk_union_st->t_liquidate_file_dtl_st.f_snd_code       , "���ͻ�����33��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_snd_code    ),
        "f_rcv_code"        , chk_union_st->t_liquidate_file_dtl_st.f_rcv_code        , "���ܻ�����100��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_rcv_code     ),
        "f_iss_code"      ,  chk_union_st->t_liquidate_file_dtl_st.f_iss_code      , "������������" ,   PTS_DB_TYPE_CHAR ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_iss_code   ),
        "f_trace_no"      , chk_union_st->t_liquidate_file_dtl_st.f_trace_no      , "ϵͳ���ٺţ�11��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_trace_no   ),
        "f_tran_time"   , chk_union_st->t_liquidate_file_dtl_st.f_tran_time   , "����ʱ�䣨7��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_time),
        "f_card_no"   , chk_union_st->t_liquidate_file_dtl_st.f_card_no   , "���ţ�2��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_card_no),
        "f_tran_amt"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_amt   , "���׽��" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_amt),
        "f_tran_type"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_type   , "ϵͳ��������" ,   PTS_DB_TYPE_INT  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_type),
        "f_mcc"   , chk_union_st->t_liquidate_file_dtl_st.f_mcc   , "mcc" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_mcc),
        "f_merchant_no"   , chk_union_st->t_liquidate_file_dtl_st.f_merchant_no   , "�̻��ţ�42��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_merchant_no),
        "f_terminal_no"   , chk_union_st->t_liquidate_file_dtl_st.f_terminal_no   , "�ն˺ţ�41��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_terminal_no),
        "f_tran_rrn"   , chk_union_st->t_liquidate_file_dtl_st.f_tran_rrn   , "���ײο��ţ�37��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_rrn),
        "f_auth_no"   , chk_union_st->t_liquidate_file_dtl_st.f_auth_no   , "��ȨӦ���루38��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_auth_no),
        "f_msg_code"   , chk_union_st->t_liquidate_file_dtl_st.f_msg_code   , "��������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_msg_code),
        "f_tran_code"   , chk_union_st->t_liquidate_file_dtl_st.f_tran_code   , "���������루3��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_code),
        "f_sub_tran_code"   , chk_union_st->t_liquidate_file_dtl_st.f_sub_tran_code   , "����������루25��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_sub_tran_code),
        "f_resp_code"   , chk_union_st->t_liquidate_file_dtl_st.f_resp_code   , "���׷����루39��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_resp_code),
        "f_rcv_fee"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_fee   , "ת�ӷ���ѣ�X+n11��" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_fee),
        "f_pay_fee"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_fee   , "ת�ӷ���ѣ�X+n11��" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_fee),
        "f_tran_fee"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_fee   , "ת�ӷ���ѣ�X+n11��" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_fee),
        "f_org_trace_no"   , chk_union_st->t_liquidate_file_dtl_st.f_org_trace_no   , "ԭʼ����ϵͳ���ٺţ�90.2��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_org_trace_no),
        "f_org_tran_time"   , chk_union_st->t_liquidate_file_dtl_st.f_org_tran_time   , "ԭʼ��������ʱ�䣨90.3��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_org_tran_time),
        "f_data_acom"   , chk_union_st->t_liquidate_file_dtl_st.f_data_acom   , "�����ļ�һ����" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_data_acom),
        "f_check_flag"   , chk_union_st->t_liquidate_file_dtl_st.f_check_flag   , "���˱�ʶ" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_check_flag),
        "f_check_date"   , chk_union_st->t_liquidate_file_dtl_st.f_check_date   , "��������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_check_date),
        "f_comments"   , chk_union_st->t_liquidate_file_dtl_st.f_comments   , "��ע" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_comments),
        "f_file_date"   , chk_union_st->t_liquidate_file_dtl_st.f_file_date   , "�ļ�����" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_file_date),
        ""               , NULL                                 , NULL       ,   0                ,  0};

    iRet = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);
    if (iRet < 0)
    {
        return (iRet);
    }
    
    strcpy( chk_union_st->f_trace_no, chk_union_st->t_liquidate_file_dtl_st.f_trace_no );
    strcpy( chk_union_st->f_tran_time, chk_union_st->t_liquidate_file_dtl_st.f_tran_time );
    
    return iRet;
}

/**
 *��Ƕ��������ϵͳ�ټ�¼
 *����t_liquidate_file_dtl
 **/
int db_check_file_dtl(MYSQL *conn_ptr)
{
    CHK_UNION_ST    chk_union_st;
    DB_RESULT       *db_result = NULL;
    int             iRet, i ;
    char            sql[1024]     = { 0 };
    char            sql_info[512] = { 0 };
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    /*׼���α�*/
    strcpy (sql_info    , "SELECT T_LIQUIDATE_FILE_DTL");
    sprintf (sql,
            "SELECT * "
            "  FROM T_LIQUIDATE_FILE_DTL t "
            " WHERE t.f_check_flag = \'%s\' "
            "   AND t.f_file_date = \'%s\' "
            "   AND t.f_channel_no = %d "
            , FUNDCHNL_CHK_STATUS_INIT
            , szDailyDate
            , CHANNEL_CUP_DAIFU
    );
    
    /*ִ���α�*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "��Ҫ�������ˮ����[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        memset( &chk_union_st, 0x00, sizeof( CHK_UNION_ST ) );
        /*ƥ��ϵͳ������ˮ��¼*/
        iRet = db_get_file_dtl_info(db_result, &chk_union_st, sql_info);
        if (iRet == -2)
        {
            SysLog( LOGTYPE_INFO , "��������" );
            break;
        }

        iRet = db_update_df_data(conn_ptr, &chk_union_st, FUNDCHNL_CHK_STATUS_OK);
        if ( iRet < 0 )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return ERROR;
        }
        else
        if ( iRet == 0 )
        {
            /**
             *δƥ�䵽ϵͳ��ļ�¼
             *����df_data�����ټ�¼��ʶ
             *�Ǽ�ϵͳ����
             **/
            iRet = db_update_file_dtl(conn_ptr, &chk_union_st, FUNDCHNL_CHK_STATUS_LESS);
            if ( iRet <= 0 )
            {
                db_rollback (conn_ptr);
                db_free_result(db_result);
                return ERROR;
            }
            
            iRet = db_prepare_err_info(conn_ptr, &chk_union_st, FUNDCHNL_CHK_STATUS_LESS);
            if ( iRet < 0 )
            {
                db_rollback (conn_ptr);
                db_free_result(db_result);
                return ERROR;
            }
            
            /*�Ǽǲ��*/
            iRet = check_err(conn_ptr, &chk_union_st);
            if ( NOERR != iRet )
            {
                SysLog( LOGTYPE_ERROR , "�Ǽǲ��ʧ��" );
                return ERROR;
            }
        }
        else
        {
            /**
             *ƥ�䵽ϵͳ��ļ�¼
             *����df_data�����ټ�¼��ʶ
             *�Ǽ�ϵͳ����
             **/
            iRet = db_update_file_dtl(conn_ptr, &chk_union_st, FUNDCHNL_CHK_STATUS_OK);
            if ( iRet <= 0 )
            {
                db_rollback (conn_ptr);
                db_free_result(db_result);
                return ERROR;
            }
        }
    }
    
    db_free_result(db_result);
    
    return NOERR;
}

/**
 *ȡϵͳ�೬������������ȻΪ�����ϵ���ˮ��¼
 **/
int db_get_df_data_info(DB_RESULT * db_result, CHK_UNION_ST *chk_union_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "ID"       , &chk_union_st->df_data_st.ID       , "ID",   PTS_DB_TYPE_LONG ,  sizeof(chk_union_st->df_data_st.ID    ),
        "ORDER_ID"       , chk_union_st->df_data_st.ORDER_ID       , "������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ORDER_ID    ),
        "TXN_TIME"       , chk_union_st->df_data_st.TXN_TIME       , "����ʱ��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TXN_TIME    ),
        "CUSTOMER_NM"       , chk_union_st->df_data_st.CUSTOMER_NM       , "����" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CUSTOMER_NM    ),
        "BANK_NUM"        , chk_union_st->df_data_st.BANK_NUM        , "����" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.BANK_NUM     ),
        "TXN_AMT"      ,  chk_union_st->df_data_st.TXN_AMT      , "���׽��" ,   PTS_DB_TYPE_CHAR ,  sizeof(chk_union_st->df_data_st.TXN_AMT   ),
        "ACC_TYPE"      , chk_union_st->df_data_st.ACC_TYPE      , "�˻�����" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ACC_TYPE   ),
        "CHECK_FLAG"   , chk_union_st->df_data_st.CHECK_FLAG   , "���״̬��0��ʼ��1.���ͨ����2.��˲�ͨ��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CHECK_FLAG),
        "CHECK_COMMENT"   , chk_union_st->df_data_st.CHECK_COMMENT   , "�����ע" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CHECK_COMMENT),
        "TRAN_FLAG"   , chk_union_st->df_data_st.TRAN_FLAG   , "״̬" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TRAN_FLAG),
        "ERROR_CODE"   , chk_union_st->df_data_st.ERROR_CODE   , "������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ERROR_CODE),
        "ERROR_MSG"   , chk_union_st->df_data_st.ERROR_MSG   , "������Ϣ" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ERROR_MSG),
        "CREATE_BY"   , chk_union_st->df_data_st.CREATE_BY   , "������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CREATE_BY),
        "CREATE_DATE"   , chk_union_st->df_data_st.CREATE_DATE   , "����ʱ��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CREATE_DATE),
        "UPDATE_BY"   , chk_union_st->df_data_st.UPDATE_BY   , "������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.UPDATE_BY),
        "UPDATE_DATE"   , chk_union_st->df_data_st.UPDATE_DATE   , "����ʱ��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.UPDATE_DATE),
        "SHBILL_ID"   , &chk_union_st->df_data_st.SHBILL_ID   , "��������Ψһ��ʶ" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->df_data_st.SHBILL_ID),
        "SHOP_NO"   , chk_union_st->df_data_st.SHOP_NO   , "�̻����" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.SHOP_NO),
        "SHOP_NAME"   , chk_union_st->df_data_st.SHOP_NAME   , "�̻�����" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.SHOP_NAME),
        "SETTLE_DATE"   , &chk_union_st->df_data_st.SETTLE_DATE   , "��������" ,   PTS_DB_TYPE_INT  ,  sizeof(chk_union_st->df_data_st.SETTLE_DATE),
        "ACCT_TIME"   , &chk_union_st->df_data_st.ACCT_TIME   , "��ת���ڣ�����רԱ�������£� ��ʼ0��" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->df_data_st.ACCT_TIME),
        "SHOP_MAP_ID"   , chk_union_st->df_data_st.SHOP_MAP_ID   , "�̻�ID" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.SHOP_MAP_ID),
        "TRACE_NO"   , chk_union_st->df_data_st.TRACE_NO   , "ϵͳ���ٺ�" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TRACE_NO),
        "TRACE_TIME"   , chk_union_st->df_data_st.TRACE_TIME   , "���״���ʱ��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TRACE_TIME),
        "CHECK_DATE"   , chk_union_st->df_data_st.CHECK_DATE   , "��������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CHECK_DATE),
        ""               , NULL                                 , NULL       ,   0                ,  0};

    iRet = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);
    if (iRet < 0)
    {
        return (iRet);
    }
    
    strcpy( chk_union_st->f_trace_no, chk_union_st->df_data_st.TRACE_NO );
    strcpy( chk_union_st->f_tran_time, chk_union_st->df_data_st.TRACE_TIME );
    
    return iRet;
}

/**
 *���ϵͳ���¼
 *����df_data��
 **/
int db_check_df_data(MYSQL *conn_ptr)
{
    CHK_UNION_ST     chk_union_st;
    DB_RESULT       *db_result = NULL;
    int             iRet, i ;
    char            sql[1024]     = { 0 };
    char            sql_info[512] = { 0 };
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    /*׼���α�*/
    strcpy (sql_info    , "SELECT DF_DATA");
    sprintf (sql,
            "select * "
            "  from df_data t "
            " where t.check_flag = \'%s\' "
            "   and t.txn_time < concat(date_format(str_to_date(\'%s\', '%%Y%%m%%d') - %d, '%%Y%%m%%d'), '000000') "
            , FUNDCHNL_CHK_STATUS_INIT
            , szDailyDate
            , MAX_DEFF_DAYS
    );
    
    /*ִ���α�*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "��Ҫ�������ˮ����[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        memset( &chk_union_st, 0x00, sizeof( CHK_UNION_ST ) );
        /*ƥ��ϵͳ������ˮ��¼*/
        iRet = db_get_df_data_info(db_result, &chk_union_st, sql_info);
        if (iRet < 0)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return ERROR;
        }
        else if ( iRet == 0 )
        {
            break;
        }
        
        iRet = db_update_df_data(conn_ptr, &chk_union_st, FUNDCHNL_CHK_STATUS_MORE);
        if ( iRet <= 0 )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return ERROR;
        }
        
        iRet = db_prepare_err_info(conn_ptr, &chk_union_st, FUNDCHNL_CHK_STATUS_MORE);
        if ( iRet < 0 )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return ERROR;
        }
            
        /*�Ǽǲ��*/
        iRet = check_err(conn_ptr, &chk_union_st);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "�Ǽǲ��ʧ��" );
            return ERROR;
        }
    }
    
    db_free_result(db_result);
    
    return NOERR;
}
/**
 *���˿��ƺ���
 **/
int check_exec(MYSQL *conn_ptr)
{
    int             iRet ;
    
    /*���˳�ʼ��*/
    iRet = db_check_file_dtl(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "�����ļ���ˮʧ��" );
        return ERROR;
    }
    
    /*���˳�ʼ��*/
    iRet = db_check_df_data(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "����ϵͳ��ˮʧ��" );
        return ERROR;
    }
    
    return NOERR;
}


/**
 *���˽���
 **/
int check_end(MYSQL *conn_ptr)
{
    char  sql_exec[1024]  = { 0 };
    char  sql_info[512]   = { 0 };
    int   iRet            = -1;
    long sys_cnt, bal_cnt, err1_cnt, err2_cnt;
    long sys_amt, bal_amt, err1_amt, err2_amt;
    
    DB_GET_ST data_bal [] = {
        "cnt"            , &bal_cnt    , "�����������"  , PTS_DB_TYPE_LONG  , sizeof(bal_cnt ) ,
        "f_tran_amt"     , &bal_amt    , "����������"  , PTS_DB_TYPE_LONG , sizeof(bal_amt) ,
        ""               , NULL             , NULL          , 0               , 0};
    DB_GET_ST data_err1 [] = {
        "cnt"            , &err1_cnt    , "ϵͳ�ټ�¼������"  , PTS_DB_TYPE_LONG  , sizeof(err1_cnt ) ,
        "f_tran_amt"     , &err1_amt    , "ϵͳ�ټ�¼�����"  , PTS_DB_TYPE_LONG , sizeof(err1_amt) ,
        ""               , NULL             , NULL          , 0               , 0};
    DB_GET_ST data_err2 [] = {
        "cnt"            , &err2_cnt    , "ϵͳ���¼������"  , PTS_DB_TYPE_LONG  , sizeof(err2_cnt ) ,
        "f_tran_amt"     , &err2_amt    , "ϵͳ���¼�����"  , PTS_DB_TYPE_LONG , sizeof(err2_amt) ,
        ""               , NULL             , NULL          , 0               , 0};
    
    /*ͳ�ƶ������*/
    strcpy (sql_info   , "SELECT T_LIQUIDATE_FILE_DTL.STAT_BAL");
    sprintf( sql_exec,
            //" select count(*) as cnt, ifnull(sum(t.f_tran_amt), 0) as f_tran_amt "
            " select count(*) as cnt, ifnull(sum(t.f_tran_amt), 0) as f_tran_amt "
            "   from t_liquidate_file_dtl t "
            "  where t.f_check_date = \'%s\' "
            "    and t.f_check_flag = \'%s\' "
            "    and t.f_channel_no = %d "
            , szDailyDate
            , FUNDCHNL_CHK_STATUS_OK
            , CHANNEL_CUP_DAIFU
    );
    iRet = db_select_one (data_bal, conn_ptr, sql_exec, sql_info, LOG_SQL_TITLE);
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ͳ�ƶ���ϵͳ�ټ�¼*/
    memset( sql_info , 0x00 , sizeof( sql_info ) );
    memset( sql_exec , 0x00 , sizeof( sql_exec ) );
    strcpy (sql_info   , "SELECT T_LIQUIDATE_FILE_DTL.STAT_ERR1");
    sprintf( sql_exec,
            " select count(*) as cnt, ifnull(sum(t.f_tran_amt), 0) as f_tran_amt "
            "   from t_liquidate_file_dtl t "
            "  where t.f_check_date = \'%s\' "
            "    and t.f_check_flag = \'%s\' "
            "    and t.f_channel_no = %d "
            , szDailyDate
            , FUNDCHNL_CHK_STATUS_LESS
            , CHANNEL_CUP_DAIFU
    );
    iRet = db_select_one (data_err1, conn_ptr, sql_exec, sql_info, LOG_SQL_TITLE);
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ͳ�ƶ���ϵͳ�ټ�¼*/
    memset( sql_info , 0x00 , sizeof( sql_info ) );
    memset( sql_exec , 0x00 , sizeof( sql_exec ) );
    strcpy (sql_info   , "SELECT DF_DATA.STAT_ERR2");
    sprintf( sql_exec,
            " select count(*) as cnt, ifnull(sum(t.txn_amt), 0) as f_tran_amt "
            "   from df_data t "
            "  where t.check_date = \'%s\' "
            "    and t.check_flag = \'%s\' "
            , szDailyDate
            , FUNDCHNL_CHK_STATUS_MORE
    );
    iRet = db_select_one (data_err2, conn_ptr, sql_exec, sql_info, LOG_SQL_TITLE);
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*����ϵͳ���¼*/
    memset( sql_exec , 0x00 , sizeof( sql_exec ) );
    sprintf( sql_exec,
            " update t_liquidate_file t "
            "    set t.f_status = %d, "
            "        t.f_check_end_time = date_format(sysdate(),'%%Y-%%m-%%d %%H:%%i:%%s'), "
            "        t.f_check_bal_cnt = %ld, "
            "        t.f_check_bal_amt = %ld, "
            "        t.f_err_cnt = %ld, "
            "        t.f_err_amt = %ld "
            "  where t.f_file_date = \'%s\' "
            "    and t.f_channel_no = %d "
            , LIQUIDATE_FILE_STATUS_CHK_SUCC
            , bal_cnt, bal_amt
            , err1_cnt + err2_cnt, err1_amt + err2_amt
            , szDailyDate
            , CHANNEL_CUP_DAIFU
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
int func_check_resp_file(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "���˿�ʼ����" );
    
    get_dayend_date( szDailyDate );
    
    /*���˳�ʼ��*/
    iRet = check_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "���˳�ʼ��ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*����*/
    iRet = check_exec(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "����ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*���˽���*/
    iRet = check_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "���˽���ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    SysLog( LOGTYPE_INFO , "���˽�������" );
    
    return STEP_EXEC_STATUS_SUCC;
}