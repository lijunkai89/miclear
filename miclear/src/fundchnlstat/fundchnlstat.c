/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 清分清算系统
 *    SystemName  : 调度步骤-资金通道数据统计
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-资金通道数据统计
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/03/20         北京           李君凯         创建文档
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"
#include "dbbase.h"

static char  szDailyDate[9]    = { 0 };
static char  db_posp_name[30]  = { 0 };

#define LOG_SQL_TITLE "银联直连通道交易统计"

typedef struct
{
    int     host_map_id             ;
    int     cut_date                    ;
    long    trans_datetime_b            ;
    long    trans_datetime_e            ;
    int     host_clear_date         ;
    char    host_name	    [90 +1] ;
    char    host_nameab	    [40 +1] ;
    char    host_clear_type	[1  +1] ;
    long    pt_trans_amt	        ;
    long    pt_shop_fee_amt	        ;
    long    pt_shop_mdr_amt         ;
    long    pt_shop_avr_amt	        ;
    long    pt_shop_net_amt         ;
    long    pt_mdr_host_amt	        ;
    long    pt_mdr_iss_amt	        ;
    long    pt_mdr_cup_amt	        ;
    long    pt_mdr_logo_amt	        ;
    long    pt_mdr_plat_amt	        ;
    long    pt_mdr_partner_amt      ;
    long    pt_settle_amt           ;
    long    pt_settle_fee           ;
    long    ht_trans_amt	        ;
    long    ht_shop_fee_amt	        ;
    long    ht_shop_mdr_amt         ;
    long    ht_shop_avr_amt         ;
    long    ht_shop_net_amt         ;
    long    ht_mdr_host_amt         ;
    long    ht_mdr_iss_amt          ;
    long    ht_mdr_cup_amt          ;
    long    ht_mdr_logo_amt         ;
    long    ht_mdr_plat_amt         ;
    long    ht_mdr_partner_amt      ;
    long    ht_settle_amt           ;
    long    ht_settle_fee           ;
    int     pt_c_cnt                ;
    long    pt_c_amt                ;
    int     pt_d_cnt                ;
    long    pt_d_amt                ;
    int     pt_total_cnt            ;
    long    pt_total_amt            ;
    int     pt_error_cnt            ;
    long    pt_error_amt            ;
    int     pt_auto_cnt             ;
    long    pt_auto_amt             ;
    int     ht_c_cnt                ;
    long    ht_c_amt                ;
    int     ht_d_cnt                ;
    long    ht_d_amt                ;
    char    check_flag      [1  +1] ;
    char    acct_flag       [1  +1] ;
    long    acct_time               ;
    int     user_id1                ;
    char    user_name1      [30 +1] ;
    int     user_id2                ;
    char    user_name2      [30 +1] ;
    char    acct_desc       [250+1] ;
    char    acct_desc1      [250+1] ;
    char    acct_desc2      [250+1] ;
    char    mac	            [32 +1] ;
    
    long     min_system_ref;
    long     max_system_ref;
}STAT_HDTOTAL_ST;

typedef struct
{
    int     host_map_id             ;
    int     cut_date                    ;
    long    trans_datetime_b            ;
    long    trans_datetime_e            ;
    int     host_clear_date         ;
    char    host_name	    [90 +1] ;
    char    host_nameab	    [40 +1] ;
    char    host_clear_type	[1  +1] ;
    long    pt_trans_amt	        ;
    long    pt_shop_fee_amt	        ;
    long    pt_shop_mdr_amt         ;
    long    pt_shop_avr_amt	        ;
    long    pt_shop_net_amt         ;
    long    pt_mdr_host_amt	        ;
    long    pt_mdr_iss_amt	        ;
    long    pt_mdr_cup_amt	        ;
    long    pt_mdr_logo_amt	        ;
    long    pt_mdr_plat_amt	        ;
    long    pt_mdr_partner_amt      ;
    long    pt_settle_amt           ;
    long    pt_settle_fee           ;
    long    ht_trans_amt	        ;
    long    ht_shop_fee_amt	        ;
    long    ht_shop_mdr_amt         ;
    long    ht_shop_avr_amt         ;
    long    ht_shop_net_amt         ;
    long    ht_mdr_host_amt         ;
    long    ht_mdr_iss_amt          ;
    long    ht_mdr_cup_amt          ;
    long    ht_mdr_logo_amt         ;
    long    ht_mdr_plat_amt         ;
    long    ht_mdr_partner_amt      ;
    long    ht_settle_amt           ;
    long    ht_settle_fee           ;
    int     pt_c_cnt                ;
    long    pt_c_amt                ;
    int     pt_d_cnt                ;
    long    pt_d_amt                ;
    int     pt_total_cnt            ;
    long    pt_total_amt            ;
    int     pt_error_cnt            ;
    long    pt_error_amt            ;
    int     pt_auto_cnt             ;
    long    pt_auto_amt             ;
    int     ht_c_cnt                ;
    long    ht_c_amt                ;
    int     ht_d_cnt                ;
    long    ht_d_amt                ;
    char    check_flag      [1  +1] ;
    char    acct_flag       [1  +1] ;
    long    acct_time               ;
    int     user_id1                ;
    char    user_name1      [30 +1] ;
    int     user_id2                ;
    char    user_name2      [30 +1] ;
    char    acct_desc       [250+1] ;
    char    acct_desc1      [250+1] ;
    char    acct_desc2      [250+1] ;
    char    mac	            [32 +1] ;
}STAT_HDBILL_ST;

/**
 *初始化
 **/
int func_fundchnl_stat_init(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "资金通道数据统计开始……" );
    
    strcpy( db_posp_name, getenv("MYSQL_POSP_NAME") );
    get_dayend_date( szDailyDate );
    
    return NOERR;
}

/**
 *取待登记流水的数据
 **/
int db_get_host_sum_info(DB_RESULT * db_result, STAT_HDTOTAL_ST *hdtotal_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "HOST_MAP_ID"        , &hdtotal_st->host_map_id       , "HOST_MAP_ID"         , PTS_DB_TYPE_INT  , sizeof(hdtotal_st->host_map_id    ),
        "MAX_SYSTEM_REF"     , &hdtotal_st->max_system_ref    , "MAX_SYSTEM_REF"      , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->max_system_ref ),
        "MIN_SYSTEM_REF"     , &hdtotal_st->min_system_ref    , "MIN_SYSTEM_REF"      , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->min_system_ref ),
        "PT_TRANS_AMT"       , &hdtotal_st->pt_trans_amt      , "PT_TRANS_AMT"        , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_trans_amt      ),
        "PT_TRANS_AMT"       , &hdtotal_st->ht_trans_amt      , "PT_TRANS_AMT"        , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_trans_amt      ),
        "PT_SHOP_FEE_AMT"    , &hdtotal_st->pt_shop_fee_amt   , "PT_SHOP_FEE_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_shop_fee_amt   ),
        "HT_SHOP_FEE_AMT"    , &hdtotal_st->ht_shop_fee_amt   , "HT_SHOP_FEE_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_shop_fee_amt   ),
        "PT_SHOP_MDR_AMT"    , &hdtotal_st->pt_shop_mdr_amt   , "PT_SHOP_MDR_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_shop_mdr_amt   ),
        "HT_SHOP_MDR_AMT"    , &hdtotal_st->ht_shop_mdr_amt   , "HT_SHOP_MDR_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_shop_mdr_amt   ),
        "PT_SHOP_AVR_AMT"    , &hdtotal_st->pt_shop_avr_amt   , "PT_SHOP_AVR_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_shop_avr_amt   ),
        "HT_SHOP_AVR_AMT"    , &hdtotal_st->ht_shop_avr_amt   , "HT_SHOP_AVR_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_shop_avr_amt   ),
        "PT_SHOP_NET_AMT"    , &hdtotal_st->pt_shop_net_amt   , "PT_SHOP_NET_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_shop_net_amt   ),
        "HT_SHOP_NET_AMT"    , &hdtotal_st->ht_shop_net_amt   , "HT_SHOP_NET_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_shop_net_amt   ),
        "PT_MDR_HOST_AMT"    , &hdtotal_st->pt_mdr_host_amt   , "PT_MDR_HOST_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_mdr_host_amt   ),
        "HT_MDR_HOST_AMT"    , &hdtotal_st->ht_mdr_host_amt   , "HT_MDR_HOST_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_mdr_host_amt   ),
        "PT_MDR_ISS_AMT"     , &hdtotal_st->pt_mdr_iss_amt    , "PT_MDR_ISS_AMT"      , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_mdr_iss_amt    ),
        "HT_MDR_ISS_AMT"     , &hdtotal_st->ht_mdr_iss_amt    , "HT_MDR_ISS_AMT"      , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_mdr_iss_amt    ),
        "PT_MDR_CUP_AMT"     , &hdtotal_st->pt_mdr_cup_amt    , "PT_MDR_CUP_AMT"      , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_mdr_cup_amt    ),
        "HT_MDR_CUP_AMT"     , &hdtotal_st->ht_mdr_cup_amt    , "HT_MDR_CUP_AMT"      , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_mdr_cup_amt    ),
        "PT_MDR_LOGO_AMT"    , &hdtotal_st->pt_mdr_logo_amt   , "PT_MDR_LOGO_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_mdr_logo_amt   ),
        "HT_MDR_LOGO_AMT"    , &hdtotal_st->ht_mdr_logo_amt   , "HT_MDR_LOGO_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_mdr_logo_amt   ),
        "PT_MDR_PLAT_AMT"    , &hdtotal_st->pt_mdr_plat_amt   , "PT_MDR_PLAT_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_mdr_plat_amt   ),
        "HT_MDR_PLAT_AMT"    , &hdtotal_st->ht_mdr_plat_amt   , "HT_MDR_PLAT_AMT"     , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_mdr_plat_amt   ),
        "PT_MDR_PARTNER_AMT" , &hdtotal_st->pt_mdr_partner_amt, "PT_MDR_PARTNER_AMT"  , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_mdr_partner_amt),
        "HT_MDR_PARTNER_AMT" , &hdtotal_st->ht_mdr_partner_amt, "HT_MDR_PARTNER_AMT"  , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_mdr_partner_amt),
        "PT_SETTLE_AMT"      , &hdtotal_st->pt_settle_amt     , "PT_SETTLE_AMT"       , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_settle_amt     ),
        "HT_SETTLE_AMT"      , &hdtotal_st->ht_settle_amt     , "HT_SETTLE_AMT"       , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_settle_amt     ),
        ""                   , NULL              , NULL                  ,   0              , 0};
        
    iRet = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);

    return iRet;
}

/**
 *检测是否已经登记
 **/
int db_select_exist_hdtotal(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;

    sprintf (sql_info   , "SELECT HDTOTAL.EXSIT_%d", hdtotal_st->host_map_id);
    sprintf (sql, 
            "SELECT HOST_MAP_ID "
            "  FROM %s.A_HDTOTAL "
            " WHERE HOST_MAP_ID = %d "
            "   AND HOST_CLEAR_DATE = %d"
            , db_posp_name
            , hdtotal_st->host_map_id
            , atoi(szDailyDate)
    );

    iRet = db_exsit_check (conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    return iRet;
}

int db_get_host_info(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char val_name[100];
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "HOST_NAMEAB"         ,  hdtotal_st->host_nameab	       , "通道简称      ", PTS_DB_TYPE_CHAR, sizeof(hdtotal_st->host_nameab	     ),
        "HOST_NAME"           ,  hdtotal_st->host_name           , "通道名称      ", PTS_DB_TYPE_CHAR, sizeof(hdtotal_st->host_name         	     ),
        "SETTLE_MODE"         ,  hdtotal_st->host_clear_type     , "结算方式      ", PTS_DB_TYPE_CHAR, sizeof(hdtotal_st->host_clear_type	     ),
        "PT_SETTLE_FEE"       , &hdtotal_st->pt_settle_fee       , "平台结算手续费", PTS_DB_TYPE_LONG, sizeof(hdtotal_st->pt_settle_fee       ),
        "HT_SETTLE_FEE"       , &hdtotal_st->ht_settle_fee       , "通道结算手续费", PTS_DB_TYPE_LONG, sizeof(hdtotal_st->ht_settle_fee       ),
        ""                    , NULL                                       , NULL        , 0               , 0 };
    
    sprintf (sql_info   , "SELECT TRACE.HDTOTAL_C_%d", hdtotal_st->host_map_id);
    sprintf (sql, 
            "SELECT HOST_NAMEAB, "
            "       HOST_NAME, "
            "       SETTLE_MODE, "
            "       SETTLE_FEE AS PT_SETTLE_FEE, "
            "       SETTLE_FEE AS HT_SETTLE_FEE "
            "  FROM %s.A_HOST "
            " WHERE HOST_MAP_ID = %d "
            , db_posp_name
            , hdtotal_st->host_map_id
    );
    
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
        
    return iRet;
}

/**
 *消费成功的交易统计
 **/
int db_stat_consum_succ(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "PT_TOTAL_AMT"    , &hdtotal_st->pt_c_amt       , "PT_TOTAL_AMT", PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_c_amt),
        "HT_TOTAL_AMT"    , &hdtotal_st->ht_c_amt       , "HT_TOTAL_AMT", PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_c_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.HDTOTAL_C_%d", hdtotal_st->host_map_id);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS PT_TOTAL_AMT, "
            "       SUM(TRANS_AMT) AS HT_TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE HOST_MAP_ID = %d "
            "   AND CUT_DATE = %d "
            "   AND TRANS_RETCODE = \'00\' "
            "   AND TRANS_STATUS = \'0\' "
            "   AND TRANS_REVERFLAG = \'0\' "
            "   AND (TRANS_ID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d) "
            , db_posp_name
            , hdtotal_st->host_map_id
            , atoi(szDailyDate)
            , APP_PURCHASE
            , APP_COMFIRM_NORMAL
            , APP_COMFIRM_ORDER
    );
    
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    /*类型转换*/
    hdtotal_st->pt_c_cnt   = (int) lcnt;
    hdtotal_st->ht_c_cnt   = (int) lcnt ;
    
    return iRet;
}

/**
 *退货成功的交易统计
 **/
int db_stat_refund_succ(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "PT_TOTAL_AMT"    , &hdtotal_st->pt_d_amt       , "PT_TOTAL_AMT", PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_d_amt),
        "HT_TOTAL_AMT"    , &hdtotal_st->ht_d_amt       , "HT_TOTAL_AMT", PTS_DB_TYPE_LONG , sizeof(hdtotal_st->ht_d_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.HDTOTAL_D_%d", hdtotal_st->host_map_id);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS PT_TOTAL_AMT, "
            "       SUM(TRANS_AMT) AS HT_TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE HOST_MAP_ID = %d "
            "   AND CUT_DATE = %d "
            "   AND TRANS_RETCODE = \'00\' "
            "   AND TRANS_STATUS = \'0\' "
            "   AND TRANS_REVERFLAG = \'0\' "
            "   AND (TRANS_ID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d) "
            , db_posp_name
            , hdtotal_st->host_map_id
            , atoi(szDailyDate)
            , APP_REFUND
            , APP_CANCEL_PURCHASE_NORMAL
            , APP_CANCEL_PURCHASE_ORDER
            , APP_CANCEL_COMFIRM_NORMAL
            , APP_CANCEL_COMFIRM_ORDER
    );
    
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    hdtotal_st->pt_d_cnt   = (int) lcnt ;
    hdtotal_st->ht_d_cnt   = (int) lcnt ;
    //金额整数显示
    hdtotal_st->pt_d_amt  *= -1;
    hdtotal_st->ht_d_amt  *= -1;
    
    return iRet;
}

/**
 *所有交易统计
 **/
int db_stat_all_type(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "TOTAL_AMT"       , &hdtotal_st->pt_total_amt    , "TOTAL_AMT"   , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_total_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.HDTOTAL_TOTAL_%d", hdtotal_st->host_map_id);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE HOST_MAP_ID = %d "
            "   AND CUT_DATE = %d "
            , db_posp_name
            , hdtotal_st->host_map_id
            , atoi(szDailyDate)
    );
    
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    hdtotal_st->pt_total_cnt = (int) lcnt ;
    
    return iRet;
}

/**
 *所有交易统计
 **/
int db_stat_all_fail(STAT_HDTOTAL_ST *hdtotal_st)
{
    hdtotal_st->pt_error_cnt   = 0 ;
    hdtotal_st->pt_error_amt   = 0 ;
}

/**
 *所有冲正交易统计
 **/
int db_stat_all_reve(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "TOTAL_AMT"       , &hdtotal_st->pt_auto_amt     , "TOTAL_AMT"   , PTS_DB_TYPE_LONG , sizeof(hdtotal_st->pt_auto_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.HDTOTAL_AUTO_%d", hdtotal_st->host_map_id);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE HOST_MAP_ID = %d "
            "   AND CUT_DATE = %d "
            "   AND TRANS_REVERFLAG = \'1\' "
            , db_posp_name
            , hdtotal_st->host_map_id
            , atoi(szDailyDate)
    );

    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    hdtotal_st->pt_auto_amt   *=  -1 ;
    hdtotal_st->pt_auto_cnt = (int) lcnt ;
    
    return iRet;
}

/**
 *查询交易时间范围-开始
 **/
int db_get_time_begin(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    long trans_date_long;
    int  trans_date, trans_time;
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "HOST_CLEAR_DATE"       , &hdtotal_st->host_clear_date      , "HOST_CLEAR_DATE   "   , PTS_DB_TYPE_INT , sizeof(hdtotal_st->host_clear_date),
        "POS_DATE"              , &trans_date                       , "POS_DATE          "   , PTS_DB_TYPE_INT , sizeof(trans_date),
        "POS_TIME"              , &trans_time                       , "POS_TIME          "   , PTS_DB_TYPE_INT , sizeof(trans_time),
        ""                      , NULL                              , NULL                   ,   0              , 0};
    
    strcpy (sql_info   , "SELECT TRACE.SD_DT_MIN");
    sprintf (sql, 
            "SELECT HOST_CLEAR_DATE, "
            "       POS_DATE, "
            "       POS_TIME "
            "  FROM %s.A_TRACE "
            " WHERE SYSTEM_REF = %ld "
            , db_posp_name
            , hdtotal_st->min_system_ref
    );

    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    //trans_date_long = (long) trans_date;
    //hdtotal_st->trans_datetime_b  =  trans_date_long * 1000000 + trans_time  ;
    hdtotal_st->trans_datetime_b  =  trans_date * 1000000 + trans_time  ;
    
    return iRet;
}

/**
 *查询交易时间范围-结束
 **/
int db_get_time_end(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    long trans_date_long;
    int  trans_date, trans_time;
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "HOST_CLEAR_DATE"       , &hdtotal_st->host_clear_date       , "HOST_CLEAR_DATE   "   , PTS_DB_TYPE_INT , sizeof(hdtotal_st->host_clear_date),
        "POS_DATE"              , &trans_date                        , "POS_DATE          "   , PTS_DB_TYPE_INT , sizeof(trans_date),
        "POS_TIME"              , &trans_time                        , "POS_TIME          "   , PTS_DB_TYPE_INT , sizeof(trans_time),
        ""                      , NULL                               , NULL                   ,   0              , 0};
    
    strcpy (sql_info   , "SELECT TRACE.SD_DT_MAX");
    sprintf (sql, 
            "SELECT HOST_CLEAR_DATE, "
            "       POS_DATE, "
            "       POS_TIME "
            "  FROM %s.A_TRACE "
            " WHERE SYSTEM_REF = %ld "
            , db_posp_name
            , hdtotal_st->max_system_ref
    );
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    //trans_date_long = (long) trans_date;
    //hdtotal_st->trans_datetime_e  =  trans_date_long * 1000000 + trans_time  ;
    hdtotal_st->trans_datetime_e  =  trans_date * 1000000 + trans_time  ;
    
    return iRet;
}

/**
 *准备其他字段
 **/
int db_put_hbtotal_other_info(STAT_HDTOTAL_ST *hdtotal_st)
{
    hdtotal_st->cut_date          =  atoi(szDailyDate)    ;
    hdtotal_st->host_map_id       =  CHANNEL_CUP_DIRECT;  /*暂且0*/

    /////////////////////////////////////////////////////////
    hdtotal_st->acct_flag[0]  = '1'; //未划款
    
    hdtotal_st->pt_settle_fee = 0;
    hdtotal_st->pt_settle_amt = hdtotal_st->pt_shop_net_amt ;

    hdtotal_st->check_flag[0] = '0' ; // 待初审
    hdtotal_st->acct_time     =  0  ;
    hdtotal_st->user_id1      =  0  ;
    hdtotal_st->user_id2      =  0  ;
    strcpy (hdtotal_st->user_name1 , "-");     // [30 +1]
    strcpy (hdtotal_st->user_name2 , "-");     // [30 +1]
    strcpy (hdtotal_st->acct_desc  , "-");     // [250+1]
    strcpy (hdtotal_st->acct_desc1 , "-");     // [250+1]
    strcpy (hdtotal_st->acct_desc2 , "-");     // [250+1]
    strcpy (hdtotal_st->mac	     , "-");     // [32 +1]
    
    return NOERR;
}

int db_insert_hdtotal(MYSQL *conn_ptr, STAT_HDTOTAL_ST *hdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "HOST_MAP_ID"                    , &hdtotal_st->host_map_id       ,   "HOST_MAP_ID        "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->host_map_id              ),
        "CUT_DATE"                       , &hdtotal_st->cut_date          ,   "CUT_DATE           "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->cut_date                 ),
        "TRANS_DATETIME_B"               , &hdtotal_st->trans_datetime_b  ,   "TRANS_DATETIME_B   "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->trans_datetime_b         ),
        "TRANS_DATETIME_E"               , &hdtotal_st->trans_datetime_e  ,   "TRANS_DATETIME_E   "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->trans_datetime_e         ),
        "HOST_CLEAR_DATE"                , &hdtotal_st->host_clear_date   ,   "HOST_CLEAR_DATE    "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->host_clear_date          ),
        "HOST_NAME"                      ,  hdtotal_st->host_name         ,   "HOST_NAME          "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->host_name                ),
        "HOST_NAMEAB"                    ,  hdtotal_st->host_nameab       ,   "HOST_NAMEAB        "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->host_nameab              ),
        "HOST_CLEAR_TYPE"                ,  hdtotal_st->host_clear_type   ,   "HOST_CLEAR_TYPE    "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->host_clear_type          ),
        "PT_TRANS_AMT"                   , &hdtotal_st->pt_trans_amt      ,   "PT_TRANS_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_trans_amt             ),
        "PT_SHOP_FEE_AMT"                , &hdtotal_st->pt_shop_fee_amt   ,   "PT_SHOP_FEE_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_shop_fee_amt          ),
        "PT_SHOP_MDR_AMT"                , &hdtotal_st->pt_shop_mdr_amt   ,   "PT_SHOP_MDR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_shop_mdr_amt          ),
        "PT_SHOP_AVR_AMT"                , &hdtotal_st->pt_shop_avr_amt   ,   "PT_SHOP_AVR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_shop_avr_amt          ),
        "PT_SHOP_NET_AMT"                , &hdtotal_st->pt_shop_net_amt   ,   "PT_SHOP_NET_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_shop_net_amt          ),
        "PT_MDR_HOST_AMT"                , &hdtotal_st->pt_mdr_host_amt   ,   "PT_MDR_HOST_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_mdr_host_amt          ),
        "PT_MDR_ISS_AMT"                 , &hdtotal_st->pt_mdr_iss_amt    ,   "PT_MDR_ISS_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_mdr_iss_amt           ),
        "PT_MDR_CUP_AMT"                 , &hdtotal_st->pt_mdr_cup_amt    ,   "PT_MDR_CUP_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_mdr_cup_amt           ),
        "PT_MDR_LOGO_AMT"                , &hdtotal_st->pt_mdr_logo_amt   ,   "PT_MDR_LOGO_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_mdr_logo_amt          ),
        "PT_MDR_PLAT_AMT"                , &hdtotal_st->pt_mdr_plat_amt   ,   "PT_MDR_PLAT_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_mdr_plat_amt          ),
        "PT_MDR_PARTNER_AMT"             , &hdtotal_st->pt_mdr_partner_amt,   "PT_MDR_PARTNER_AMT "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_mdr_partner_amt       ),
        "PT_SETTLE_AMT"                  , &hdtotal_st->pt_settle_amt     ,   "PT_SETTLE_AMT      "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_settle_amt            ),
        "PT_SETTLE_FEE"                  , &hdtotal_st->pt_settle_fee     ,   "PT_SETTLE_FEE      "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_settle_fee            ),
        "HT_TRANS_AMT"                   , &hdtotal_st->ht_trans_amt      ,   "HT_TRANS_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_trans_amt             ),
        "HT_SHOP_FEE_AMT"                , &hdtotal_st->ht_shop_fee_amt   ,   "HT_SHOP_FEE_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_shop_fee_amt          ),
        "HT_SHOP_MDR_AMT"                , &hdtotal_st->ht_shop_mdr_amt   ,   "HT_SHOP_MDR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_shop_mdr_amt          ),
        "HT_SHOP_AVR_AMT"                , &hdtotal_st->ht_shop_avr_amt   ,   "HT_SHOP_AVR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_shop_avr_amt          ),
        "HT_SHOP_NET_AMT"                , &hdtotal_st->ht_shop_net_amt   ,   "HT_SHOP_NET_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_shop_net_amt          ),
        "HT_MDR_HOST_AMT"                , &hdtotal_st->ht_mdr_host_amt   ,   "HT_MDR_HOST_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_mdr_host_amt          ),
        "HT_MDR_ISS_AMT"                 , &hdtotal_st->ht_mdr_iss_amt    ,   "HT_MDR_ISS_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_mdr_iss_amt           ),
        "HT_MDR_CUP_AMT"                 , &hdtotal_st->ht_mdr_cup_amt    ,   "HT_MDR_CUP_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_mdr_cup_amt           ),
        "HT_MDR_LOGO_AMT"                , &hdtotal_st->ht_mdr_logo_amt   ,   "HT_MDR_LOGO_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_mdr_logo_amt          ),
        "HT_MDR_PLAT_AMT"                , &hdtotal_st->ht_mdr_plat_amt   ,   "HT_MDR_PLAT_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_mdr_plat_amt          ),
        "HT_MDR_PARTNER_AMT"             , &hdtotal_st->ht_mdr_partner_amt,   "HT_MDR_PARTNER_AMT "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_mdr_partner_amt       ),
        "HT_SETTLE_AMT"                  , &hdtotal_st->ht_settle_amt     ,   "HT_SETTLE_AMT      "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_settle_amt            ),
        "HT_SETTLE_FEE"                  , &hdtotal_st->ht_settle_fee     ,   "HT_SETTLE_FEE      "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_settle_fee            ),
        "PT_C_CNT"                       , &hdtotal_st->pt_c_cnt          ,   "PT_C_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->pt_c_cnt ),
        "PT_C_AMT"                       , &hdtotal_st->pt_c_amt          ,   "PT_C_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_c_amt ),
        "PT_D_CNT"                       , &hdtotal_st->pt_d_cnt          ,   "PT_D_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->pt_d_cnt ),
        "PT_D_AMT"                       , &hdtotal_st->pt_d_amt          ,   "PT_D_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_d_amt ),
        "PT_TOTAL_CNT"                   , &hdtotal_st->pt_total_cnt      ,   "PT_TOTAL_CNT       "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->pt_total_cnt             ),
        "PT_TOTAL_AMT"                   , &hdtotal_st->pt_total_amt      ,   "PT_TOTAL_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_total_amt             ),
        "PT_ERROR_CNT"                   , &hdtotal_st->pt_error_cnt      ,   "PT_ERROR_CNT       "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->pt_error_cnt             ),
        "PT_ERROR_AMT"                   , &hdtotal_st->pt_error_amt      ,   "PT_ERROR_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_error_amt             ),
        "PT_AUTO_CNT"                    , &hdtotal_st->pt_auto_cnt       ,   "PT_AUTO_CNT        "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->pt_auto_cnt              ),
        "PT_AUTO_AMT"                    , &hdtotal_st->pt_auto_amt       ,   "PT_AUTO_AMT        "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->pt_auto_amt              ),
        "HT_C_CNT"                       , &hdtotal_st->ht_c_cnt          ,   "HT_C_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->ht_c_cnt                 ),
        "HT_C_AMT"                       , &hdtotal_st->ht_c_amt          ,   "HT_C_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_c_amt                 ),
        "HT_D_CNT"                       , &hdtotal_st->ht_d_cnt          ,   "HT_D_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->ht_d_cnt                 ),
        "HT_D_AMT"                       , &hdtotal_st->ht_d_amt          ,   "HT_D_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->ht_d_amt                 ),
        "CHECK_FLAG"                     ,  hdtotal_st->check_flag        ,   "CHECK_FLAG         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->check_flag               ),
        "ACCT_FLAG"                      ,  hdtotal_st->acct_flag         ,   "ACCT_FLAG          "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->acct_flag                ),
        "ACCT_TIME"                      , &hdtotal_st->acct_time         ,   "ACCT_TIME          "     , PTS_DB_TYPE_LONG ,    sizeof (hdtotal_st->acct_time                ),
        "USER_ID1"                       , &hdtotal_st->user_id1          ,   "USER_ID1           "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->user_id1                 ),
        "USER_NAME1"                     ,  hdtotal_st->user_name1        ,   "USER_NAME1         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->user_name1               ),
        "USER_ID2"                       , &hdtotal_st->user_id2          ,   "USER_ID2           "     , PTS_DB_TYPE_INT  ,    sizeof (hdtotal_st->user_id2                 ),
        "USER_NAME2"                     ,  hdtotal_st->user_name2        ,   "USER_NAME2         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->user_name2               ),
        "ACCT_DESC"                      ,  hdtotal_st->acct_desc         ,   "ACCT_DESC          "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->acct_desc                ),
        "ACCT_DESC1"                     ,  hdtotal_st->acct_desc1        ,   "ACCT_DESC1         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->acct_desc1               ),
        "ACCT_DESC2"                     ,  hdtotal_st->acct_desc2        ,   "ACCT_DESC2         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->acct_desc2               ),
        "MAC"                            ,  hdtotal_st->mac               ,   "MAC                "     , PTS_DB_TYPE_CHAR ,    sizeof (hdtotal_st->mac                      ),
        ""                               ,  NULL                        ,   NULL                      , 0                ,   0 };
        
    strcpy (sql_info   , "INSERT A_HDTOTAL");
    sprintf(sql        , "INSERT INTO %s.A_HDTOTAL SET ", db_posp_name);
    
    iRet = db_insert_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    return (iRet);
}

/**
 *统计汇总
 **/
int func_fundchnl_stat_sum(MYSQL *conn_ptr)
{
    int  i, iRet;
    char sql[2048]    = { 0 };
    char sql_info[512]= { 0 };
    STAT_HDTOTAL_ST hdtotal_st;
    DB_RESULT       *db_result = NULL;
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    //1. 按照HOST_MAP_ID分组，获取交易金额汇总
    sprintf (sql_info   , "SELECT TRACE_HDTOTAL");
    sprintf (sql,
            "SELECT HOST_MAP_ID, "
            "       MAX(SYSTEM_REF) AS MAX_SYSTEM_REF, "
            "       MIN(SYSTEM_REF) AS MIN_SYSTEM_REF, "
            "       SUM(TRANS_AMT) AS PT_TRANS_AMT, "
            "       SUM(TRANS_AMT) AS HT_TRANS_AMT, "
            "       SUM(SHOP_FEE_AMT) AS PT_SHOP_FEE_AMT, "
            "       SUM(SHOP_FEE_AMT) AS HT_SHOP_FEE_AMT, "
            "       SUM(SHOP_MDR_AMT) AS PT_SHOP_MDR_AMT, "
            "       SUM(SHOP_MDR_AMT) AS HT_SHOP_MDR_AMT, "
            "       SUM(SHOP_AVR_AMT) AS PT_SHOP_AVR_AMT, "
            "       SUM(SHOP_AVR_AMT) AS HT_SHOP_AVR_AMT, "
            "       SUM(SHOP_NET_AMT) AS PT_SHOP_NET_AMT, "
            "       SUM(SHOP_NET_AMT) AS HT_SHOP_NET_AMT, "
            "       SUM(MDR_HOST_AMT) AS PT_MDR_HOST_AMT, "
            "       SUM(MDR_HOST_AMT) AS HT_MDR_HOST_AMT, "
            "       SUM(MDR_ISS_AMT) AS PT_MDR_ISS_AMT, "
            "       SUM(MDR_ISS_AMT) AS HT_MDR_ISS_AMT, "
            "       SUM(MDR_CUP_AMT) AS PT_MDR_CUP_AMT, "
            "       SUM(MDR_CUP_AMT) AS HT_MDR_CUP_AMT, "
            "       SUM(MDR_LOGO_AMT) AS PT_MDR_LOGO_AMT, "
            "       SUM(MDR_LOGO_AMT) AS HT_MDR_LOGO_AMT, "
            "       SUM(MDR_PLAT_AMT) AS PT_MDR_PLAT_AMT, "
            "       SUM(MDR_PLAT_AMT) AS HT_MDR_PLAT_AMT, "
            "       SUM(MDR_PARTNER_AMT) AS PT_MDR_PARTNER_AMT, "
            "       SUM(MDR_PARTNER_AMT) AS HT_MDR_PARTNER_AMT, "
            "       SUM(SETTLE_PLAT_AMT) AS PT_SETTLE_AMT, "
            "       SUM(SETTLE_PLAT_AMT) AS HT_SETTLE_AMT "
            "  FROM %s.A_TRACE "
            " WHERE CUT_DATE = %d "
            "   AND TRANS_RETCODE = \'00\' "
            "   AND TRANS_STATUS = \'0\' "
            "   AND TRANS_REVERFLAG = \'0\' "
            "   AND TRANS_AMT != \'0\' "
            "   AND HOST_MAP_ID = %d "
            " GROUP BY HOST_MAP_ID "
            " ORDER BY HOST_MAP_ID"
            , db_posp_name
            , atoi(szDailyDate)
            , CHANNEL_CUP_DIRECT
    );
    
    /*执行游标*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "需要处理的流水笔数[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        /*获取流水数据*/
        memset (&hdtotal_st, 0, sizeof(STAT_HDTOTAL_ST));
        iRet = db_get_host_sum_info(db_result, &hdtotal_st, sql_info);
        if (iRet == -2)
        {
            SysLog( LOGTYPE_INFO , "遍历结束" );
            break;
        }

        /*系统流水重复校验*/
        iRet = db_select_exist_hdtotal(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        else
        if (iRet > 0)
        {
            //已登记过，不再登记
            SysLog(LOGTYPE_INFO, "通道id[%d] 日切日期[%s].已经日终生成日汇总完成.WARNNING!", hdtotal_st.host_map_id, szDailyDate);
            continue;
        }
        
        /*查询通道信息*/
        iRet = db_get_host_info(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*消费成功统计*/
        iRet = db_stat_consum_succ(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*退货成功统计*/
        iRet = db_stat_refund_succ(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*所有交易统计*/
        iRet = db_stat_all_type(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*所有失败交易统计*/
        db_stat_all_fail(&hdtotal_st);
        
        /*所有冲正交易统计*/
        iRet = db_stat_all_reve(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        /*交易日期时间的范围-开始*/
        iRet = db_get_time_begin(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        /*交易日期时间的范围-结束*/
        iRet = db_get_time_end(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*交易日期时间的范围-结束*/
        iRet = db_put_hbtotal_other_info(&hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*登记商户日汇总*/
        iRet = db_insert_hdtotal(conn_ptr, &hdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        db_commit(conn_ptr);
        
        SysLog( LOGTYPE_DEBUG , "NO.[%d].通道ID[%d] 日切日期[%s].日终生成日汇总单.生成成功,SUCC!", i+1, hdtotal_st.host_map_id, szDailyDate);
    }
    SysLog( LOGTYPE_INFO , "处理完成" );
    db_free_result(db_result);
    
    return NOERR;
}

int db_get_host_total(DB_RESULT * db_result, STAT_HDBILL_ST *hdbill_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };
    long lpt_c_cnt, lpt_d_cnt, lpt_total_cnt, lpt_error_cnt, lpt_auto_cnt, lht_c_cnt, lht_d_cnt;
    
    DB_GET_ST data [] = {
        "HOST_MAP_ID"                    , &hdbill_st->host_map_id       ,   "HOST_MAP_ID         "    , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->host_map_id           ),
        "CUT_DATE"                       , &hdbill_st->cut_date          ,   "CUT_DATE            "    , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->cut_date           ),
        "HOST_CLEAR_DATE"                , &hdbill_st->host_clear_date   ,   "HOST_CLEAR_DATE     "    , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->host_clear_date           ),
        "MIN_TRANS_DATETIME_B"           , &hdbill_st->trans_datetime_b  ,   "MIN_TRANS_DATETIMEB "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->trans_datetime_b      ),
        "MAX_TRANS_DATETIME_E"           , &hdbill_st->trans_datetime_e  ,   "MAX_TRANS_DATETIMEE "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->trans_datetime_e      ),
        "PTS_TRANS_AMT"                  , &hdbill_st->pt_trans_amt      ,   "PTS_TRANS_AMT       "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_trans_amt          ),
        "PTS_SHOP_FEE_AMT"               , &hdbill_st->pt_shop_fee_amt   ,   "PTS_SHOP_FEE_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_fee_amt       ),
        "PTS_SHOP_MDR_AMT"               , &hdbill_st->pt_shop_mdr_amt   ,   "PTS_SHOP_MDR_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_mdr_amt       ),
        "PTS_SHOP_AVR_AMT"               , &hdbill_st->pt_shop_avr_amt   ,   "PTS_SHOP_AVR_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_avr_amt       ),
        "PTS_SHOP_NET_AMT"               , &hdbill_st->pt_shop_net_amt   ,   "PTS_SHOP_NET_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_net_amt       ),
        "PTS_MDR_HOST_AMT"               , &hdbill_st->pt_mdr_host_amt   ,   "PTS_MDR_HOST_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_host_amt       ),
        "PTS_MDR_ISS_AMT"                , &hdbill_st->pt_mdr_iss_amt    ,   "PTS_MDR_ISS_AMT     "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_iss_amt        ),
        "PTS_MDR_CUP_AMT"                , &hdbill_st->pt_mdr_cup_amt    ,   "PTS_MDR_CUP_AMT     "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_cup_amt        ),
        "PTS_MDR_LOGO_AMT"               , &hdbill_st->pt_mdr_logo_amt   ,   "PTS_MDR_LOGO_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_logo_amt       ),
        "PTS_MDR_PLAT_AMT"               , &hdbill_st->pt_mdr_plat_amt   ,   "PTS_MDR_PLAT_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_plat_amt       ),
        "PTS_MDR_PARTNER_AMT"            , &hdbill_st->pt_mdr_partner_amt,   "PTS_MDR_PARTNER_AMT "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_partner_amt    ),
        "PTS_SETTLE_AMT"                 , &hdbill_st->pt_settle_amt     ,   "PTS_SETTLE_AMT      "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_settle_amt         ),
        "PTS_SETTLE_FEE"                 , &hdbill_st->pt_settle_fee     ,   "PTS_SETTLE_FEE      "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_settle_fee         ),
        "HTS_TRANS_AMT"                  , &hdbill_st->ht_trans_amt      ,   "HTS_TRANS_AMT       "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_trans_amt          ),
        "HTS_SHOP_FEE_AMT"               , &hdbill_st->ht_shop_fee_amt   ,   "HTS_SHOP_FEE_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_fee_amt       ),
        "HTS_SHOP_MDR_AMT"               , &hdbill_st->ht_shop_mdr_amt   ,   "HTS_SHOP_MDR_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_mdr_amt       ),
        "HTS_SHOP_AVR_AMT"               , &hdbill_st->ht_shop_avr_amt   ,   "HTS_SHOP_AVR_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_avr_amt       ),
        "HTS_SHOP_NET_AMT"               , &hdbill_st->ht_shop_net_amt   ,   "HTS_SHOP_NET_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_net_amt       ),
        "HTS_MDR_HOST_AMT"               , &hdbill_st->ht_mdr_host_amt   ,   "HTS_MDR_HOST_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_host_amt       ),
        "HTS_MDR_ISS_AMT"                , &hdbill_st->ht_mdr_iss_amt    ,   "HTS_MDR_ISS_AMT     "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_iss_amt        ),
        "HTS_MDR_CUP_AMT"                , &hdbill_st->ht_mdr_cup_amt    ,   "HTS_MDR_CUP_AMT     "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_cup_amt        ),
        "HTS_MDR_LOGO_AMT"               , &hdbill_st->ht_mdr_logo_amt   ,   "HTS_MDR_LOGO_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_logo_amt       ),
        "HTS_MDR_PLAT_AMT"               , &hdbill_st->ht_mdr_plat_amt   ,   "HTS_MDR_PLAT_AMT    "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_plat_amt       ),
        "HTS_MDR_PARTNER_AMT"            , &hdbill_st->ht_mdr_partner_amt,   "HTS_MDR_PARTNER_AMT "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_partner_amt    ),
        "HTS_SETTLE_AMT"                 , &hdbill_st->ht_settle_amt     ,   "HTS_SETTLE_AMT      "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_settle_amt         ),
        "HTS_SETTLE_FEE"                 , &hdbill_st->ht_settle_fee     ,   "HTS_SETTLE_FEE      "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_settle_fee         ),
        "PTS_C_CNT"                      , &lpt_c_cnt                    ,   "PTS_C_CNT           "    , PTS_DB_TYPE_LONG ,    sizeof (lpt_c_cnt              ),
        "PTS_C_AMT"                      , &hdbill_st->pt_c_amt          ,   "PTS_C_AMT           "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_c_amt              ),
        "PTS_D_CNT"                      , &lpt_d_cnt                    ,   "PTS_D_CNT           "    , PTS_DB_TYPE_LONG ,    sizeof (lpt_d_cnt              ),
        "PTS_D_AMT"                      , &hdbill_st->pt_d_amt          ,   "PTS_D_AMT           "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_d_amt              ),
        "PTS_TOTAL_CNT"                  , &lpt_total_cnt                ,   "PTS_TOTAL_CNT       "    , PTS_DB_TYPE_LONG ,    sizeof (lpt_total_cnt          ),
        "PTS_TOTAL_AMT"                  , &hdbill_st->pt_total_amt      ,   "PTS_TOTAL_AMT       "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_total_amt          ),
        "PTS_ERROR_CNT"                  , &lpt_error_cnt                ,   "PTS_ERROR_CNT       "    , PTS_DB_TYPE_LONG ,    sizeof (lpt_error_cnt          ),
        "PTS_ERROR_AMT"                  , &hdbill_st->pt_error_amt      ,   "PTS_ERROR_AMT       "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_error_amt          ),
        "PTS_AUTO_CNT"                   , &lpt_auto_cnt                ,   "PTS_AUTO_CNT        "    , PTS_DB_TYPE_LONG ,    sizeof (lpt_auto_cnt           ),
        "PTS_AUTO_AMT"                   , &hdbill_st->pt_auto_amt       ,   "PTS_AUTO_AMT        "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_auto_amt           ),
        "HTS_C_CNT"                      , &lht_c_cnt                    ,   "HTS_C_CNT           "    , PTS_DB_TYPE_LONG ,    sizeof (lht_c_cnt              ),
        "HTS_C_AMT"                      , &hdbill_st->ht_c_amt          ,   "HTS_C_AMT           "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_c_amt              ),
        "HTS_D_CNT"                      , &lht_d_cnt                    ,   "HTS_D_CNT           "    , PTS_DB_TYPE_LONG ,    sizeof (lht_d_cnt              ),
        "HTS_D_AMT"                      , &hdbill_st->ht_d_amt          ,   "HTS_D_AMT           "    , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_d_amt              ),
        ""                               , NULL                          ,   NULL                      , 0                ,    0};
   
    iRet = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);
    
    /*转换数据类型*/
    hdbill_st->pt_c_cnt = (int) lpt_c_cnt;
    hdbill_st->pt_d_cnt = (int) lpt_d_cnt;
    hdbill_st->pt_total_cnt = (int) lpt_total_cnt;
    hdbill_st->pt_error_cnt = (int) lpt_error_cnt;
    hdbill_st->pt_auto_cnt = (int) lpt_auto_cnt;
    hdbill_st->ht_c_cnt = (int) lht_c_cnt;
    hdbill_st->ht_d_cnt = (int) lht_d_cnt;
    
    return iRet;
}

/**
 *准备结算单其他字段
 **/
int db_put_hdbill_other_info(STAT_HDBILL_ST *hdbill_st)
{
    hdbill_st->check_flag[0] = '1' ;
    hdbill_st->acct_time      =  0  ;
    hdbill_st->user_id1       =  0  ;
    hdbill_st->user_id2       =  0  ;
    strcpy (hdbill_st->user_name1 , "-");     // [30 +1]
    strcpy (hdbill_st->user_name2 , "-");     // [30 +1]
    strcpy (hdbill_st->acct_desc  , "-");     // [250+1]
    strcpy (hdbill_st->acct_desc1 , "-");     // [250+1]
    strcpy (hdbill_st->acct_desc2 , "-");     // [250+1]
    strcpy (hdbill_st->mac	     , "-");     // [32 +1]
        
    return NOERR;
}


/**
 *检测是否已经登记
 **/
int db_select_exist_hdbill(MYSQL *conn_ptr, STAT_HDBILL_ST *hdbill_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;

    sprintf (sql_info   , "SELECT A_HDBILL.EXSIT_%d", hdbill_st->host_map_id);
    sprintf (sql, 
            "SELECT HOST_MAP_ID "
            "  FROM %s.A_HDBILL "
            " WHERE HOST_MAP_ID = %d "
            "   AND HOST_CLEAR_DATE = %d"
            , db_posp_name
            , hdbill_st->host_map_id
            , atoi(szDailyDate)
    );

    iRet = db_exsit_check (conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    return iRet;
}

/**
 *登记商户日汇总
 **/
int db_insert_hdbill(MYSQL *conn_ptr, STAT_HDBILL_ST *hdbill_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "HOST_MAP_ID"                    , &hdbill_st->host_map_id       ,   "HOST_MAP_ID        "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->host_map_id              ),
        "CUT_DATE"                       , &hdbill_st->cut_date          ,   "CUT_DATE           "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->cut_date                 ),
        "TRANS_DATETIME_B"               , &hdbill_st->trans_datetime_b  ,   "TRANS_DATETIME_B   "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->trans_datetime_b         ),
        "TRANS_DATETIME_E"               , &hdbill_st->trans_datetime_e  ,   "TRANS_DATETIME_E   "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->trans_datetime_e         ),
        "HOST_CLEAR_DATE"                , &hdbill_st->host_clear_date   ,   "HOST_CLEAR_DATE    "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->host_clear_date          ),
        "HOST_NAME"                      ,  hdbill_st->host_name         ,   "HOST_NAME          "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->host_name                ),
        "HOST_NAMEAB"                    ,  hdbill_st->host_nameab       ,   "HOST_NAMEAB        "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->host_nameab              ),
        "HOST_CLEAR_TYPE"                ,  hdbill_st->host_clear_type   ,   "HOST_CLEAR_TYPE    "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->host_clear_type          ),
        "PT_TRANS_AMT"                   , &hdbill_st->pt_trans_amt      ,   "PT_TRANS_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_trans_amt             ),
        "PT_SHOP_FEE_AMT"                , &hdbill_st->pt_shop_fee_amt   ,   "PT_SHOP_FEE_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_fee_amt          ),
        "PT_SHOP_MDR_AMT"                , &hdbill_st->pt_shop_mdr_amt   ,   "PT_SHOP_MDR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_mdr_amt          ),
        "PT_SHOP_AVR_AMT"                , &hdbill_st->pt_shop_avr_amt   ,   "PT_SHOP_AVR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_avr_amt          ),
        "PT_SHOP_NET_AMT"                , &hdbill_st->pt_shop_net_amt   ,   "PT_SHOP_NET_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_shop_net_amt          ),
        "PT_MDR_HOST_AMT"                , &hdbill_st->pt_mdr_host_amt   ,   "PT_MDR_HOST_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_host_amt          ),
        "PT_MDR_ISS_AMT"                 , &hdbill_st->pt_mdr_iss_amt    ,   "PT_MDR_ISS_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_iss_amt           ),
        "PT_MDR_CUP_AMT"                 , &hdbill_st->pt_mdr_cup_amt    ,   "PT_MDR_CUP_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_cup_amt           ),
        "PT_MDR_LOGO_AMT"                , &hdbill_st->pt_mdr_logo_amt   ,   "PT_MDR_LOGO_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_logo_amt          ),
        "PT_MDR_PLAT_AMT"                , &hdbill_st->pt_mdr_plat_amt   ,   "PT_MDR_PLAT_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_plat_amt          ),
        "PT_MDR_PARTNER_AMT"             , &hdbill_st->pt_mdr_partner_amt,   "PT_MDR_PARTNER_AMT "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_mdr_partner_amt       ),
        "PT_SETTLE_AMT"                  , &hdbill_st->pt_settle_amt     ,   "PT_SETTLE_AMT      "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_settle_amt            ),
        "PT_SETTLE_FEE"                  , &hdbill_st->pt_settle_fee     ,   "PT_SETTLE_FEE      "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_settle_fee            ),
        "HT_TRANS_AMT"                   , &hdbill_st->ht_trans_amt      ,   "HT_TRANS_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_trans_amt             ),
        "HT_SHOP_FEE_AMT"                , &hdbill_st->ht_shop_fee_amt   ,   "HT_SHOP_FEE_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_fee_amt          ),
        "HT_SHOP_MDR_AMT"                , &hdbill_st->ht_shop_mdr_amt   ,   "HT_SHOP_MDR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_mdr_amt          ),
        "HT_SHOP_AVR_AMT"                , &hdbill_st->ht_shop_avr_amt   ,   "HT_SHOP_AVR_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_avr_amt          ),
        "HT_SHOP_NET_AMT"                , &hdbill_st->ht_shop_net_amt   ,   "HT_SHOP_NET_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_shop_net_amt          ),
        "HT_MDR_HOST_AMT"                , &hdbill_st->ht_mdr_host_amt   ,   "HT_MDR_HOST_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_host_amt          ),
        "HT_MDR_ISS_AMT"                 , &hdbill_st->ht_mdr_iss_amt    ,   "HT_MDR_ISS_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_iss_amt           ),
        "HT_MDR_CUP_AMT"                 , &hdbill_st->ht_mdr_cup_amt    ,   "HT_MDR_CUP_AMT     "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_cup_amt           ),
        "HT_MDR_LOGO_AMT"                , &hdbill_st->ht_mdr_logo_amt   ,   "HT_MDR_LOGO_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_logo_amt          ),
        "HT_MDR_PLAT_AMT"                , &hdbill_st->ht_mdr_plat_amt   ,   "HT_MDR_PLAT_AMT    "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_plat_amt          ),
        "HT_MDR_PARTNER_AMT"             , &hdbill_st->ht_mdr_partner_amt,   "HT_MDR_PARTNER_AMT "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_mdr_partner_amt       ),
        "HT_SETTLE_AMT"                  , &hdbill_st->ht_settle_amt     ,   "HT_SETTLE_AMT      "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_settle_amt            ),
        "HT_SETTLE_FEE"                  , &hdbill_st->ht_settle_fee     ,   "HT_SETTLE_FEE      "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_settle_fee            ),
        "PT_C_CNT"                       , &hdbill_st->pt_c_cnt          ,   "PT_C_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->pt_c_cnt ),
        "PT_C_AMT"                       , &hdbill_st->pt_c_amt          ,   "PT_C_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_c_amt ),
        "PT_D_CNT"                       , &hdbill_st->pt_d_cnt          ,   "PT_D_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->pt_d_cnt ),
        "PT_D_AMT"                       , &hdbill_st->pt_d_amt          ,   "PT_D_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_d_amt ),
        "PT_TOTAL_CNT"                   , &hdbill_st->pt_total_cnt      ,   "PT_TOTAL_CNT       "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->pt_total_cnt             ),
        "PT_TOTAL_AMT"                   , &hdbill_st->pt_total_amt      ,   "PT_TOTAL_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_total_amt             ),
        "PT_ERROR_CNT"                   , &hdbill_st->pt_error_cnt      ,   "PT_ERROR_CNT       "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->pt_error_cnt             ),
        "PT_ERROR_AMT"                   , &hdbill_st->pt_error_amt      ,   "PT_ERROR_AMT       "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_error_amt             ),
        "PT_AUTO_CNT"                    , &hdbill_st->pt_auto_cnt       ,   "PT_AUTO_CNT        "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->pt_auto_cnt              ),
        "PT_AUTO_AMT"                    , &hdbill_st->pt_auto_amt       ,   "PT_AUTO_AMT        "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->pt_auto_amt              ),
        "HT_C_CNT"                       , &hdbill_st->ht_c_cnt          ,   "HT_C_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->ht_c_cnt                 ),
        "HT_C_AMT"                       , &hdbill_st->ht_c_amt          ,   "HT_C_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_c_amt                 ),
        "HT_D_CNT"                       , &hdbill_st->ht_d_cnt          ,   "HT_D_CNT           "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->ht_d_cnt                 ),
        "HT_D_AMT"                       , &hdbill_st->ht_d_amt          ,   "HT_D_AMT           "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->ht_d_amt                 ),
        "CHECK_FLAG"                     ,  hdbill_st->check_flag        ,   "CHECK_FLAG         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->check_flag               ),
        "ACCT_FLAG"                      ,  hdbill_st->acct_flag         ,   "ACCT_FLAG          "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->acct_flag                ),
        "ACCT_TIME"                      , &hdbill_st->acct_time         ,   "ACCT_TIME          "     , PTS_DB_TYPE_LONG ,    sizeof (hdbill_st->acct_time                ),
        "USER_ID1"                       , &hdbill_st->user_id1          ,   "USER_ID1           "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->user_id1                 ),
        "USER_NAME1"                     ,  hdbill_st->user_name1        ,   "USER_NAME1         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->user_name1               ),
        "USER_ID2"                       , &hdbill_st->user_id2          ,   "USER_ID2           "     , PTS_DB_TYPE_INT  ,    sizeof (hdbill_st->user_id2                 ),
        "USER_NAME2"                     ,  hdbill_st->user_name2        ,   "USER_NAME2         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->user_name2               ),
        "ACCT_DESC"                      ,  hdbill_st->acct_desc         ,   "ACCT_DESC          "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->acct_desc                ),
        "ACCT_DESC1"                     ,  hdbill_st->acct_desc1        ,   "ACCT_DESC1         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->acct_desc1               ),
        "ACCT_DESC2"                     ,  hdbill_st->acct_desc2        ,   "ACCT_DESC2         "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->acct_desc2               ),
        "MAC"                            ,  hdbill_st->mac               ,   "MAC                "     , PTS_DB_TYPE_CHAR ,    sizeof (hdbill_st->mac                      ),
        ""                               ,  NULL                        ,   NULL                      , 0                ,   0 };
        
    strcpy (sql_info   , "INSERT A_HDBILL");
    sprintf(sql        , "INSERT INTO %s.A_HDBILL SET ", db_posp_name);
    
    iRet = db_insert_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    return (iRet);
}

/**
 *统计结算单
 **/
int func_fundchnl_stat_settle(MYSQL *conn_ptr)
{
    int  i, iRet;
    char sql[2048]    = { 0 };
    char sql_info[512]= { 0 };
    STAT_HDBILL_ST  hdbill_st;
    DB_RESULT       *db_result = NULL;
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    sprintf (sql_info   , "SELECT A_HDTOTAL");
    sprintf (sql,
            "SELECT HOST_MAP_ID, "
                  " CUT_DATE, "
                  " HOST_CLEAR_DATE, "
                  " MIN(TRANS_DATETIME_B) AS MIN_TRANS_DATETIME_B, "
                  " MAX(TRANS_DATETIME_E) AS MAX_TRANS_DATETIME_E, "
                  " SUM(PT_TRANS_AMT) AS PTS_TRANS_AMT, "
                  " SUM(PT_SHOP_FEE_AMT) AS PTS_SHOP_FEE_AMT, "
                  " SUM(PT_SHOP_MDR_AMT) AS PTS_SHOP_MDR_AMT, "
                  " SUM(PT_SHOP_AVR_AMT) AS PTS_SHOP_AVR_AMT, "
                  " SUM(PT_SHOP_NET_AMT) AS PTS_SHOP_NET_AMT, "
                  " SUM(PT_MDR_HOST_AMT) AS PTS_MDR_HOST_AMT, "
                  " SUM(PT_MDR_ISS_AMT) AS PTS_MDR_ISS_AMT, "
                  " SUM(PT_MDR_CUP_AMT) AS PTS_MDR_CUP_AMT, "
                  " SUM(PT_MDR_LOGO_AMT) AS PTS_MDR_LOGO_AMT, "
                  " SUM(PT_MDR_PLAT_AMT) AS PTS_MDR_PLAT_AMT, "
                  " SUM(PT_MDR_PARTNER_AMT) AS PTS_MDR_PARTNER_AMT, "
                  " SUM(PT_SETTLE_AMT) AS PTS_SETTLE_AMT, "
                  " SUM(PT_SETTLE_FEE) AS PTS_SETTLE_FEE, "
                  " SUM(HT_TRANS_AMT) AS HTS_TRANS_AMT, "
                  " SUM(HT_SHOP_FEE_AMT) AS HTS_SHOP_FEE_AMT, "
                  " SUM(HT_SHOP_MDR_AMT) AS HTS_SHOP_MDR_AMT, "
                  " SUM(HT_SHOP_AVR_AMT) AS HTS_SHOP_AVR_AMT, "
                  " SUM(HT_SHOP_NET_AMT) AS HTS_SHOP_NET_AMT, "
                  " SUM(HT_MDR_HOST_AMT) AS HTS_MDR_HOST_AMT, "
                  " SUM(HT_MDR_ISS_AMT) AS HTS_MDR_ISS_AMT, "
                  " SUM(HT_MDR_CUP_AMT) AS HTS_MDR_CUP_AMT, "
                  " SUM(HT_MDR_LOGO_AMT) AS HTS_MDR_LOGO_AMT, "
                  " SUM(HT_MDR_PLAT_AMT) AS HTS_MDR_PLAT_AMT, "
                  " SUM(HT_MDR_PARTNER_AMT) AS HTS_MDR_PARTNER_AMT, "
                  " SUM(HT_SETTLE_AMT) AS HTS_SETTLE_AMT, "
                  " SUM(HT_SETTLE_FEE) AS HTS_SETTLE_FEE, "
                  " SUM(PT_C_CNT) AS PTS_C_CNT, "
                  " SUM(PT_C_AMT) AS PTS_C_AMT, "
                  " SUM(PT_D_CNT) AS PTS_D_CNT, "
                  " SUM(PT_D_AMT) AS PTS_D_AMT, "
                  " SUM(PT_TOTAL_CNT) AS PTS_TOTAL_CNT, "
                  " SUM(PT_TOTAL_AMT) AS PTS_TOTAL_AMT, "
                  " SUM(PT_ERROR_CNT) AS PTS_ERROR_CNT, "
                  " SUM(PT_ERROR_AMT) AS PTS_ERROR_AMT, "
                  " SUM(PT_AUTO_CNT) AS PTS_AUTO_CNT, "
                  " SUM(PT_AUTO_AMT) AS PTS_AUTO_AMT, "
                  " SUM(HT_C_CNT) AS HTS_C_CNT, "
                  " SUM(HT_C_AMT) AS HTS_C_AMT, "
                  " SUM(HT_D_CNT) AS HTS_D_CNT, "
                  " SUM(HT_D_AMT) AS HTS_D_AMT "
            "  FROM %s.A_HDTOTAL "
            " WHERE HOST_CLEAR_DATE = %d "
            " GROUP BY HOST_MAP_ID, "
            "          CUT_DATE, "
            "          HOST_CLEAR_DATE "
            " ORDER BY HOST_MAP_ID "
            , db_posp_name
            , atoi(szDailyDate)
    );

    /*执行游标*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "需要处理的流水笔数[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        /*获取流水数据*/
        memset (&hdbill_st, 0, sizeof(STAT_HDBILL_ST));
        iRet = db_get_host_total(db_result, &hdbill_st, sql_info);
        if (iRet == -2)
        {
            SysLog( LOGTYPE_INFO , "遍历结束" );
            break;
        }

        /*系统流水重复校验*/
        iRet = db_select_exist_hdbill(conn_ptr, &hdbill_st);
        if (iRet < 0 && iRet != SQLNOFOUND)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        else
        if (iRet > 0)
        {
            //已登记过，不再登记
            SysLog(LOGTYPE_INFO, "通道[%d] 日切日期[%s].已经日终生成结算安完成.WARNNING!", hdbill_st.host_map_id, szDailyDate);
            continue;
        }
        
        /*交易日期时间的范围-结束*/
        iRet = db_put_hdbill_other_info(&hdbill_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*登记商户结算单*/
        iRet = db_insert_hdbill(conn_ptr, &hdbill_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        db_commit(conn_ptr);
        
        SysLog( LOGTYPE_DEBUG , "NO.[%d].通道[%d] 日切日期[%s].日终生成结算单.生成成功,SUCC!", i+1, hdbill_st.host_map_id, szDailyDate);
    }
    
    db_free_result(db_result);
    
    return NOERR;
}

/**
 *资金通道数据统计
 **/
int func_fundchnl_stat_exec(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    iRet = func_fundchnl_stat_sum(conn_ptr);
    if ( NOERR != iRet )
    {
        return ERROR;
    }
    
    iRet = func_fundchnl_stat_settle(conn_ptr);
    if ( NOERR != iRet )
    {
        return ERROR;
    }
    
    return NOERR;
}

/**
 *结束
 **/
int func_fundchnl_stat_end(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "资金通道数据统计结束……" );
    
    return NOERR;
}

/**
 *入口函数
 **/
int func_fundchnl_stat(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    /*登记流水初始化*/
    iRet = func_fundchnl_stat_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "资金通道数据统计初始化失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*登记流水*/
    iRet = func_fundchnl_stat_exec(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "资金通道数据统计失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*登记流水结束*/
    iRet = func_fundchnl_stat_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "资金通道数据统计结束失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    return STEP_EXEC_STATUS_SUCC;
}