/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 清分清算系统
 *    SystemName  : 调度步骤-代付对账
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-代付对账
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/11         北京           李君凯         创建文档(待修改对账流程)
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "dbbase.h"
#include "mysql.h"

/*对账最大差异日*/
#define MAX_DEFF_DAYS 2
#define LOG_SQL_TITLE "代付对账"

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
    char  sPk_id[21]         = { 0 };    /*流水文件导入主键*/
    int   iRet               = -1;
    
    /*获取流水id*/
    iRet = gen_dtl_id(conn_ptr, "seq_liquidate_err_id", sPk_id);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "获取流水id失败！" );
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
 *登记差错
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
 *初始化函数
 **/
int check_init(MYSQL *conn_ptr)
{
    char sql_exec[1024] = { 0 };
    int   iRet               = -1;
    
    /*初始化清算文件流水对账标识*/
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
    
    /*初始化代付流水表*/
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
    
    /*初始化对账统计数据*/
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
    
    /*删除当日产生的差错*/
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
 *匹配并更新系统侧流水表
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
 *更新文件流水对账结果
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
 *取待登记流水的数据
 **/
int db_get_file_dtl_info(DB_RESULT * db_result, CHK_UNION_ST *chk_union_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "f_id"       , &chk_union_st->t_liquidate_file_dtl_st.f_id       , "业务id",   PTS_DB_TYPE_LONG ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_id    ),
        "f_channel_no"       , &chk_union_st->t_liquidate_file_dtl_st.f_channel_no       , "通道编号" ,   PTS_DB_TYPE_INT  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_channel_no    ),
        "f_agent_code"       , chk_union_st->t_liquidate_file_dtl_st.f_agent_code       , "代理机构编码（32域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_agent_code    ),
        "f_snd_code"       , chk_union_st->t_liquidate_file_dtl_st.f_snd_code       , "发送机构（33域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_snd_code    ),
        "f_rcv_code"        , chk_union_st->t_liquidate_file_dtl_st.f_rcv_code        , "接受机构（100域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_rcv_code     ),
        "f_iss_code"      ,  chk_union_st->t_liquidate_file_dtl_st.f_iss_code      , "发卡机构编码" ,   PTS_DB_TYPE_CHAR ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_iss_code   ),
        "f_trace_no"      , chk_union_st->t_liquidate_file_dtl_st.f_trace_no      , "系统跟踪号（11域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_trace_no   ),
        "f_tran_time"   , chk_union_st->t_liquidate_file_dtl_st.f_tran_time   , "传输时间（7域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_time),
        "f_card_no"   , chk_union_st->t_liquidate_file_dtl_st.f_card_no   , "卡号（2域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_card_no),
        "f_tran_amt"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_amt   , "交易金额" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_amt),
        "f_tran_type"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_type   , "系统交易类型" ,   PTS_DB_TYPE_INT  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_type),
        "f_mcc"   , chk_union_st->t_liquidate_file_dtl_st.f_mcc   , "mcc" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_mcc),
        "f_merchant_no"   , chk_union_st->t_liquidate_file_dtl_st.f_merchant_no   , "商户号（42域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_merchant_no),
        "f_terminal_no"   , chk_union_st->t_liquidate_file_dtl_st.f_terminal_no   , "终端号（41域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_terminal_no),
        "f_tran_rrn"   , chk_union_st->t_liquidate_file_dtl_st.f_tran_rrn   , "交易参考号（37域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_rrn),
        "f_auth_no"   , chk_union_st->t_liquidate_file_dtl_st.f_auth_no   , "授权应答码（38域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_auth_no),
        "f_msg_code"   , chk_union_st->t_liquidate_file_dtl_st.f_msg_code   , "报文类型" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_msg_code),
        "f_tran_code"   , chk_union_st->t_liquidate_file_dtl_st.f_tran_code   , "交易类型码（3域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_code),
        "f_sub_tran_code"   , chk_union_st->t_liquidate_file_dtl_st.f_sub_tran_code   , "服务店条件码（25域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_sub_tran_code),
        "f_resp_code"   , chk_union_st->t_liquidate_file_dtl_st.f_resp_code   , "交易返回码（39域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_resp_code),
        "f_rcv_fee"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_fee   , "转接服务费（X+n11）" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_fee),
        "f_pay_fee"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_fee   , "转接服务费（X+n11）" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_fee),
        "f_tran_fee"   , &chk_union_st->t_liquidate_file_dtl_st.f_tran_fee   , "转接服务费（X+n11）" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_tran_fee),
        "f_org_trace_no"   , chk_union_st->t_liquidate_file_dtl_st.f_org_trace_no   , "原始交易系统跟踪号（90.2域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_org_trace_no),
        "f_org_tran_time"   , chk_union_st->t_liquidate_file_dtl_st.f_org_tran_time   , "原始交易日期时间（90.3域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_org_tran_time),
        "f_data_acom"   , chk_union_st->t_liquidate_file_dtl_st.f_data_acom   , "清算文件一整行" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_data_acom),
        "f_check_flag"   , chk_union_st->t_liquidate_file_dtl_st.f_check_flag   , "对账标识" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_check_flag),
        "f_check_date"   , chk_union_st->t_liquidate_file_dtl_st.f_check_date   , "对账日期" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_check_date),
        "f_comments"   , chk_union_st->t_liquidate_file_dtl_st.f_comments   , "备注" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_comments),
        "f_file_date"   , chk_union_st->t_liquidate_file_dtl_st.f_file_date   , "文件日期" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->t_liquidate_file_dtl_st.f_file_date),
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
 *标记对账相符、系统少记录
 *更新t_liquidate_file_dtl
 **/
int db_check_file_dtl(MYSQL *conn_ptr)
{
    CHK_UNION_ST    chk_union_st;
    DB_RESULT       *db_result = NULL;
    int             iRet, i ;
    char            sql[1024]     = { 0 };
    char            sql_info[512] = { 0 };
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    /*准备游标*/
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
    
    /*执行游标*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "需要处理的流水笔数[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        memset( &chk_union_st, 0x00, sizeof( CHK_UNION_ST ) );
        /*匹配系统代付流水记录*/
        iRet = db_get_file_dtl_info(db_result, &chk_union_st, sql_info);
        if (iRet == -2)
        {
            SysLog( LOGTYPE_INFO , "遍历结束" );
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
             *未匹配到系统侧的记录
             *更新df_data对账少记录标识
             *登记系统差错表
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
            
            /*登记差错*/
            iRet = check_err(conn_ptr, &chk_union_st);
            if ( NOERR != iRet )
            {
                SysLog( LOGTYPE_ERROR , "登记差错失败" );
                return ERROR;
            }
        }
        else
        {
            /**
             *匹配到系统侧的记录
             *更新df_data对账少记录标识
             *登记系统差错表
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
 *取系统侧超过最大差异日仍然为勾兑上的流水记录
 **/
int db_get_df_data_info(DB_RESULT * db_result, CHK_UNION_ST *chk_union_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "ID"       , &chk_union_st->df_data_st.ID       , "ID",   PTS_DB_TYPE_LONG ,  sizeof(chk_union_st->df_data_st.ID    ),
        "ORDER_ID"       , chk_union_st->df_data_st.ORDER_ID       , "订单号" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ORDER_ID    ),
        "TXN_TIME"       , chk_union_st->df_data_st.TXN_TIME       , "发送时间" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TXN_TIME    ),
        "CUSTOMER_NM"       , chk_union_st->df_data_st.CUSTOMER_NM       , "姓名" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CUSTOMER_NM    ),
        "BANK_NUM"        , chk_union_st->df_data_st.BANK_NUM        , "卡号" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.BANK_NUM     ),
        "TXN_AMT"      ,  chk_union_st->df_data_st.TXN_AMT      , "交易金额" ,   PTS_DB_TYPE_CHAR ,  sizeof(chk_union_st->df_data_st.TXN_AMT   ),
        "ACC_TYPE"      , chk_union_st->df_data_st.ACC_TYPE      , "账户类型" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ACC_TYPE   ),
        "CHECK_FLAG"   , chk_union_st->df_data_st.CHECK_FLAG   , "审核状态：0初始，1.审核通过，2.审核不通过" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CHECK_FLAG),
        "CHECK_COMMENT"   , chk_union_st->df_data_st.CHECK_COMMENT   , "审核批注" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CHECK_COMMENT),
        "TRAN_FLAG"   , chk_union_st->df_data_st.TRAN_FLAG   , "状态" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TRAN_FLAG),
        "ERROR_CODE"   , chk_union_st->df_data_st.ERROR_CODE   , "错误码" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ERROR_CODE),
        "ERROR_MSG"   , chk_union_st->df_data_st.ERROR_MSG   , "错误信息" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.ERROR_MSG),
        "CREATE_BY"   , chk_union_st->df_data_st.CREATE_BY   , "创建者" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CREATE_BY),
        "CREATE_DATE"   , chk_union_st->df_data_st.CREATE_DATE   , "创建时间" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CREATE_DATE),
        "UPDATE_BY"   , chk_union_st->df_data_st.UPDATE_BY   , "更新者" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.UPDATE_BY),
        "UPDATE_DATE"   , chk_union_st->df_data_st.UPDATE_DATE   , "更新时间" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.UPDATE_DATE),
        "SHBILL_ID"   , &chk_union_st->df_data_st.SHBILL_ID   , "导入数据唯一标识" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->df_data_st.SHBILL_ID),
        "SHOP_NO"   , chk_union_st->df_data_st.SHOP_NO   , "商户编号" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.SHOP_NO),
        "SHOP_NAME"   , chk_union_st->df_data_st.SHOP_NAME   , "商户名称" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.SHOP_NAME),
        "SETTLE_DATE"   , &chk_union_st->df_data_st.SETTLE_DATE   , "清算日期" ,   PTS_DB_TYPE_INT  ,  sizeof(chk_union_st->df_data_st.SETTLE_DATE),
        "ACCT_TIME"   , &chk_union_st->df_data_st.ACCT_TIME   , "划转日期（财务专员操作更新； 初始0）" ,   PTS_DB_TYPE_LONG  ,  sizeof(chk_union_st->df_data_st.ACCT_TIME),
        "SHOP_MAP_ID"   , chk_union_st->df_data_st.SHOP_MAP_ID   , "商户ID" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.SHOP_MAP_ID),
        "TRACE_NO"   , chk_union_st->df_data_st.TRACE_NO   , "系统跟踪号" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TRACE_NO),
        "TRACE_TIME"   , chk_union_st->df_data_st.TRACE_TIME   , "交易传输时间" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.TRACE_TIME),
        "CHECK_DATE"   , chk_union_st->df_data_st.CHECK_DATE   , "对账日期" ,   PTS_DB_TYPE_CHAR  ,  sizeof(chk_union_st->df_data_st.CHECK_DATE),
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
 *标记系统多记录
 *更新df_data表
 **/
int db_check_df_data(MYSQL *conn_ptr)
{
    CHK_UNION_ST     chk_union_st;
    DB_RESULT       *db_result = NULL;
    int             iRet, i ;
    char            sql[1024]     = { 0 };
    char            sql_info[512] = { 0 };
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    /*准备游标*/
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
    
    /*执行游标*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "需要处理的流水笔数[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        memset( &chk_union_st, 0x00, sizeof( CHK_UNION_ST ) );
        /*匹配系统代付流水记录*/
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
            
        /*登记差错*/
        iRet = check_err(conn_ptr, &chk_union_st);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "登记差错失败" );
            return ERROR;
        }
    }
    
    db_free_result(db_result);
    
    return NOERR;
}
/**
 *对账控制函数
 **/
int check_exec(MYSQL *conn_ptr)
{
    int             iRet ;
    
    /*对账初始化*/
    iRet = db_check_file_dtl(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "遍历文件流水失败" );
        return ERROR;
    }
    
    /*对账初始化*/
    iRet = db_check_df_data(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "遍历系统流水失败" );
        return ERROR;
    }
    
    return NOERR;
}


/**
 *对账结束
 **/
int check_end(MYSQL *conn_ptr)
{
    char  sql_exec[1024]  = { 0 };
    char  sql_info[512]   = { 0 };
    int   iRet            = -1;
    long sys_cnt, bal_cnt, err1_cnt, err2_cnt;
    long sys_amt, bal_amt, err1_amt, err2_amt;
    
    DB_GET_ST data_bal [] = {
        "cnt"            , &bal_cnt    , "对账相符笔数"  , PTS_DB_TYPE_LONG  , sizeof(bal_cnt ) ,
        "f_tran_amt"     , &bal_amt    , "对账相符金额"  , PTS_DB_TYPE_LONG , sizeof(bal_amt) ,
        ""               , NULL             , NULL          , 0               , 0};
    DB_GET_ST data_err1 [] = {
        "cnt"            , &err1_cnt    , "系统少记录差错笔数"  , PTS_DB_TYPE_LONG  , sizeof(err1_cnt ) ,
        "f_tran_amt"     , &err1_amt    , "系统少记录差错金额"  , PTS_DB_TYPE_LONG , sizeof(err1_amt) ,
        ""               , NULL             , NULL          , 0               , 0};
    DB_GET_ST data_err2 [] = {
        "cnt"            , &err2_cnt    , "系统多记录差错笔数"  , PTS_DB_TYPE_LONG  , sizeof(err2_cnt ) ,
        "f_tran_amt"     , &err2_amt    , "系统多记录差错金额"  , PTS_DB_TYPE_LONG , sizeof(err2_amt) ,
        ""               , NULL             , NULL          , 0               , 0};
    
    /*统计对账相符*/
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
    
    /*统计对账系统少记录*/
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
    
    /*统计对账系统少记录*/
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
    
    /*补核系统多记录*/
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
 *入口函数
 **/
int func_check_resp_file(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "对账开始……" );
    
    get_dayend_date( szDailyDate );
    
    /*对账初始化*/
    iRet = check_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "对账初始化失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*对账*/
    iRet = check_exec(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "对账失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*对账结束*/
    iRet = check_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "对账结束失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    SysLog( LOGTYPE_INFO , "对账结束……" );
    
    return STEP_EXEC_STATUS_SUCC;
}