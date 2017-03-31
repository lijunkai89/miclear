/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 清分清算系统
 *    SystemName  : 调度步骤-商户数据统计
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-商户数据统计
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/03/21         北京           李君凯         创建文档
 *                                                     晚于settle定时任务，对其查漏结算，无host_map_id做拆分
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"
#include "dbbase.h"

#define LOG_SQL_TITLE "直连商户交易统计"

static char  szDailyDate[9]    = { 0 };
static char  db_posp_name[30]  = { 0 };

typedef struct
{
    int     shop_map_id	                ;
    char    shop_no         [15 + 1]    ;
    int     cut_date                    ;
    long    trans_datetime_b            ;
    long    trans_datetime_e            ;
    int     shop_clear_date             ;
    char    shop_nameabcn   [60 + 1]    ;
    char    shop_name       [90 + 1]    ;
    int     branch_map_id	            ;
    long    branch_code	                ;
    int     partner_map_id	            ;
    long    partner_code                ;
    char    shop_clear_type [1  + 1]    ;
    int     county_id  	                ;
    long    pt_trans_amt                ;
    long    pt_shop_fee_amt             ;
    long    pt_shop_mdr_amt             ;
    long    pt_shop_avr_amt             ;
    long    pt_shop_net_amt             ;
    long    pt_mdr_host_amt             ;
    long    pt_mdr_iss_amt              ;
    long    pt_mdr_cup_amt              ;
    long    pt_mdr_logo_amt             ;
    long    pt_mdr_plat_amt             ;
    long    pt_mdr_partner_amt          ;
    long    pt_settle_amt               ;
    long    pt_settle_fee               ;
    int     pt_c_cnt                    ;
    long    pt_c_amt                    ;
    int     pt_d_cnt                    ;
    long    pt_d_amt                    ;
    int     pt_total_cnt                ;
    long    pt_total_amt                ;
    int     pt_error_cnt                ;
    long    pt_error_amt                ;
    int     pt_auto_cnt                 ;
    long    pt_auto_amt                 ;
    long    pt_t0_amt                   ;
    long    pt_t0_fee                   ;
    int     pt_t0_cnt                   ;
    char    check_flag          [1  + 1];
    char    acct_flag           [1  + 1];
    long    acct_time                   ;
    char    ticket_flag         [1  + 1];
    int     user_id1                    ;
    char    user_name1          [30 + 1];
    int     user_id2                    ;
    char    user_name2          [30 + 1];
    char    acct_desc           [250+ 1];
    char    acct_desc1          [250+ 1];
    char    acct_desc2          [250+ 1];
    char    mac	                [32 + 1];
    char    collect_flag	    [1  + 1];
    char    acct_type	        [1  + 1];
    char    acct_name	        [120+ 1];
    char    acct_no	            [60 + 1];
    char    bank_bid	        [20+1]  ;
    char    bid_name	        [120+ 1];
    char    bank_id	           [20+1] ;
    char    bank_name	        [60 + 1];
    int     bank_county_id	            ;
    char    bank_count_name	    [30 + 1];
    char    bank_city_name	    [30 + 1];
    char    bank_province_name  [30 + 1];
    
    long     min_system_ref;
    long     max_system_ref;
}STAT_SDTOTAL_ST;

typedef struct
{
    int     shop_map_id	                ;
    char    shop_no         [15 + 1]    ;
    int     cut_date                    ;
    long    trans_datetime_b            ;
    long    trans_datetime_e            ;
    int     shop_clear_date             ;
    char    shop_nameabcn   [60 + 1]    ;
    char    shop_name       [90 + 1]    ;
    int     branch_map_id	            ;
    long    branch_code	                ;
    int     partner_map_id	            ;
    long    partner_code                ;
    char    shop_clear_type [1  + 1]    ;
    int     county_id  	                ;
    long    pt_trans_amt                ;
    long    pt_shop_fee_amt             ;
    long    pt_shop_mdr_amt             ;
    long    pt_shop_avr_amt             ;
    long    pt_shop_net_amt             ;
    long    pt_mdr_host_amt             ;
    long    pt_mdr_iss_amt              ;
    long    pt_mdr_cup_amt              ;
    long    pt_mdr_logo_amt             ;
    long    pt_mdr_plat_amt             ;
    long    pt_mdr_partner_amt          ;
    long    pt_settle_amt               ;
    long    pt_settle_fee               ;
    int     pt_c_cnt                    ;
    long    pt_c_amt                    ;
    int     pt_d_cnt                    ;
    long    pt_d_amt                    ;
    int     pt_total_cnt                ;
    long    pt_total_amt                ;
    int     pt_error_cnt                ;
    long    pt_error_amt                ;
    int     pt_auto_cnt                 ;
    long    pt_auto_amt                 ;
    long    pt_t0_amt                   ;
    long    pt_t0_fee                   ;
    int     pt_t0_cnt                   ;
    char    check_flag          [1  + 1];
    char    acct_flag           [1  + 1];
    long    acct_time                   ;
    char    ticket_flag         [1  + 1];
    int     user_id1                    ;
    char    user_name1          [30 + 1];
    int     user_id2                    ;
    char    user_name2          [30 + 1];
    char    acct_desc           [250+ 1];
    char    acct_desc1          [250+ 1];
    char    acct_desc2          [250+ 1];
    char    mac	                [32 + 1];
    char    collect_flag	    [1  + 1];
    char    acct_type	        [1  + 1];
    char    acct_name	        [120+ 1];
    char    acct_no	            [60 + 1];
    char    bank_bid	        [20+1];
    char    bid_name	        [120+ 1];
    char    bank_id	          [20+1] ;
    char    bank_name	        [60 + 1];
    int     bank_county_id	            ;
    char    bank_count_name	    [30 + 1];
    char    bank_city_name	    [30 + 1];
    char    bank_province_name  [30 + 1];
}STAT_SDBILL_ST;

/**
 *初始化
 **/
int func_merchant_stat_init(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "商户数据统计开始……" );
    
    get_dayend_date( szDailyDate );
    strcpy( db_posp_name, getenv("MYSQL_POSP_NAME") );
    
    return NOERR;
}

/**
 *取待登记流水的数据
 **/
int db_get_shop_county_info(DB_RESULT * db_result, STAT_SDTOTAL_ST *sdtotal_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "SHOP_NO"            ,  sdtotal_st->shop_no             , "SHOP_NO           "     , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->shop_no        ),
        "SHOP_NAME"          ,  sdtotal_st->shop_name           , "SHOP_NAME         "     , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->shop_name        ),
        "SHOP_NAMEAB"        ,  sdtotal_st->shop_nameabcn       , "SHOP_NAMEAB       "     , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->shop_nameabcn        ),
        "COUNTY_ID"          , &sdtotal_st->county_id           , "COUNTY_ID         "     , PTS_DB_TYPE_INT  , sizeof(sdtotal_st->county_id      ),
        "BRANCH_MAP_ID"      , &sdtotal_st->branch_map_id       , "BRANCH_MAP_ID     "     , PTS_DB_TYPE_INT , sizeof(sdtotal_st->branch_map_id        ),
        "BRANCH_CODE"        , &sdtotal_st->branch_code         , "BRANCH_CODE       "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->branch_code        ),
        "PARTNER_MAP_ID"     , &sdtotal_st->partner_map_id      , "PARTNER_MAP_ID    "     , PTS_DB_TYPE_INT , sizeof(sdtotal_st->partner_map_id        ),
        "PARTNER_CODE"       , &sdtotal_st->partner_code        , "PARTNER_CODE      "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->partner_code        ),
        "MAX_SYSTEM_REF"     , &sdtotal_st->max_system_ref      , "MAX_SYSTEM_REF    "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->max_system_ref ),
        "MIN_SYSTEM_REF"     , &sdtotal_st->min_system_ref      , "MIN_SYSTEM_REF    "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->min_system_ref ),
        "PT_TRANS_AMT"       , &sdtotal_st->pt_trans_amt        , "PT_TRANS_AMT      "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_trans_amt      ),
        "PT_SHOP_FEE_AMT"    , &sdtotal_st->pt_shop_fee_amt     , "PT_SHOP_FEE_AMT   "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_shop_fee_amt   ),
        "PT_SHOP_MDR_AMT"    , &sdtotal_st->pt_shop_mdr_amt     , "PT_SHOP_MDR_AMT   "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_shop_mdr_amt   ),
        "PT_SHOP_AVR_AMT"    , &sdtotal_st->pt_shop_avr_amt     , "PT_SHOP_AVR_AMT   "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_shop_avr_amt   ),
        "PT_SHOP_NET_AMT"    , &sdtotal_st->pt_shop_net_amt     , "PT_SHOP_NET_AMT   "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_shop_net_amt   ),
        "PT_MDR_HOST_AMT"    , &sdtotal_st->pt_mdr_host_amt     , "PT_MDR_HOST_AMT   "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_mdr_host_amt   ),
        "PT_MDR_ISS_AMT"     , &sdtotal_st->pt_mdr_iss_amt      , "PT_MDR_ISS_AMT    "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_mdr_iss_amt    ),
        "PT_MDR_CUP_AMT"     , &sdtotal_st->pt_mdr_cup_amt      , "PT_MDR_CUP_AMT    "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_mdr_cup_amt    ),
        "PT_MDR_LOGO_AMT"    , &sdtotal_st->pt_mdr_logo_amt     , "PT_MDR_LOGO_AMT   "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_mdr_logo_amt   ),
        "PT_MDR_PLAT_AMT"    , &sdtotal_st->pt_mdr_plat_amt     , "PT_MDR_PLAT_AMT   "     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_mdr_plat_amt   ),
        "PT_MDR_PARTNER_AMT" , &sdtotal_st->pt_mdr_partner_amt  , "PT_MDR_PARTNER_AMT"     , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_mdr_partner_amt),
        "SHOP_CLEAR_TYPE"    , &sdtotal_st->shop_clear_type     , "商户清算方式      "     , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->shop_clear_type        ),
        ""                   , NULL              , NULL                     ,   0              , 0};
        
    iRet = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);

    return iRet;
}

/**
 *检测是否已经登记
 **/
int db_select_exist_sdtotal(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;

    sprintf (sql_info   , "SELECT A_SDTOTAL.EXSIT_%s", sdtotal_st->shop_no);
    sprintf (sql, 
            "SELECT SHOP_MAP_ID "
            "  FROM %s.A_SDTOTAL "
            " WHERE SHOP_NO = \'%s\' "
            "   AND SHOP_CLEAR_DATE = \'%s\'"
            , db_posp_name
            , sdtotal_st->shop_no
            , szDailyDate
    );

    iRet = db_exsit_check (conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    return iRet;
}

/**
 *消费成功的交易统计
 **/
int db_stat_consum_succ(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "TOTAL_AMT"       , &sdtotal_st->pt_c_amt        , "TOTAL_AMT"   , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_c_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.SDTOTAL_C_%s", sdtotal_st->shop_no);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE SHOP_NO = \'%s\' "
            "   AND CUT_DATE = \'%s\' "
            "   AND TRANS_RETCODE = \'00\' "
            "   AND TRANS_STATUS = \'0\' "
            "   AND TRANS_REVERFLAG = \'0\' "
            "   AND (TRANS_ID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d) "
            , db_posp_name
            , sdtotal_st->shop_no
            , szDailyDate
            , APP_PURCHASE
            , APP_COMFIRM_NORMAL
            , APP_COMFIRM_ORDER
    );
    
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    sdtotal_st->pt_c_cnt = (int) lcnt;
    
    return iRet;
}

/**
 *退货成功的交易统计
 **/
int db_stat_refund_succ(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "TOTAL_AMT"       , &sdtotal_st->pt_d_amt        , "TOTAL_AMT"   , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_d_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.SDTOTAL_D_%s", sdtotal_st->shop_no);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE SHOP_NO = \'%s\' "
            "   AND CUT_DATE = \'%s\' "
            "   AND TRANS_RETCODE = \'00\' "
            "   AND TRANS_STATUS = \'0\' "
            "   AND TRANS_REVERFLAG = \'0\' "
            "   AND (TRANS_ID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d OR TRANS_SUBID = %d) "
            , db_posp_name
            , sdtotal_st->shop_no
            , szDailyDate
            , APP_REFUND
            , APP_CANCEL_PURCHASE_NORMAL
            , APP_CANCEL_PURCHASE_ORDER
            , APP_CANCEL_COMFIRM_NORMAL
            , APP_CANCEL_COMFIRM_ORDER
    );
    
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    sdtotal_st->pt_d_cnt = (int) lcnt ;
    sdtotal_st->pt_d_amt *= -1;
    
    return iRet;
}

/**
 *所有交易统计
 **/
int db_stat_all_type(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "TOTAL_AMT"       , &sdtotal_st->pt_total_amt    , "TOTAL_AMT"   , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_total_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.SDTOTAL_TOTAL_%s", sdtotal_st->shop_no);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE SHOP_NO = \'%s\' "
            "   AND CUT_DATE = \'%s\' "
            , db_posp_name
            , sdtotal_st->shop_no
            , szDailyDate
    );
    
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    sdtotal_st->pt_total_cnt = (int) lcnt ;
    
    return iRet;
}

/**
 *所有交易统计
 **/
int db_stat_all_fail(STAT_SDTOTAL_ST *sdtotal_st)
{
    sdtotal_st->pt_error_cnt   = 0 ;
    sdtotal_st->pt_error_amt   = 0 ;
}

/**
 *所有冲正交易统计
 **/
int db_stat_all_reve(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    long lcnt;  /*解决数据库存储类型（int）和count()结果类型（long）不匹配问题*/
    
    DB_GET_ST data [] = {
        "TOTAL_CNT"       , &lcnt                       , "TOTAL_CNT"   , PTS_DB_TYPE_LONG , sizeof(lcnt),
        "TOTAL_AMT"       , &sdtotal_st->pt_auto_amt     , "TOTAL_AMT"   , PTS_DB_TYPE_LONG , sizeof(sdtotal_st->pt_auto_amt),
        ""                , NULL        , NULL          ,   0              , 0};

    sprintf (sql_info   , "SELECT TRACE.SDTOTAL_AUTO_%s", sdtotal_st->shop_no);
    sprintf (sql, 
            "SELECT COUNT(*) AS TOTAL_CNT, "
            "       SUM(TRANS_AMT) AS TOTAL_AMT "
            "  FROM %s.A_TRACE "
            " WHERE SHOP_NO = \'%s\' "
            "   AND CUT_DATE = \'%s\' "
            "   AND TRANS_REVERFLAG = \'1\' "
            , db_posp_name
            , sdtotal_st->shop_no
            , szDailyDate
    );

    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    sdtotal_st->pt_auto_amt   *=  -1 ;
    sdtotal_st->pt_auto_cnt = (int) lcnt ;
    
    return iRet;
}

/**
 *查询交易时间范围-开始
 **/
int db_get_time_begin(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    long trans_date_long;
    int  trans_date, trans_time;
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "SHOP_CLEAR_DATE"       , &sdtotal_st->shop_clear_date       , "SHOP_CLEAR_DATE   "   , PTS_DB_TYPE_INT , sizeof(sdtotal_st->shop_clear_date),
        "POS_DATE"              , &trans_date                       , "POS_DATE          "   , PTS_DB_TYPE_INT , sizeof(trans_date),
        "POS_TIME"              , &trans_time                       , "POS_TIME          "   , PTS_DB_TYPE_INT , sizeof(trans_time),
        ""                      , NULL                              , NULL                   ,   0              , 0};
    
    strcpy (sql_info   , "SELECT TRACE.SD_DT_MIN");
    sprintf (sql, 
            "SELECT SHOP_CLEAR_DATE, "
            "       POS_DATE, "
            "       POS_TIME "
            "  FROM %s.A_TRACE "
            " WHERE SYSTEM_REF = %ld "
            , db_posp_name
            , sdtotal_st->min_system_ref
    );

    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    //trans_date_long = (long) trans_date;
    //sdtotal_st->trans_datetime_b  =  trans_date_long * 1000000 + trans_time  ;
    sdtotal_st->trans_datetime_b  =  trans_date * 1000000 + trans_time  ;
    
    return iRet;
}

/**
 *查询交易时间范围-结束
 **/
int db_get_time_end(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    long trans_date_long;
    int  trans_date, trans_time;
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "SHOP_CLEAR_DATE"       , &sdtotal_st->shop_clear_date        , "SHOP_CLEAR_DATE   "   , PTS_DB_TYPE_INT , sizeof(sdtotal_st->shop_clear_date),
        "POS_DATE"              , &trans_date                        , "POS_DATE          "   , PTS_DB_TYPE_INT , sizeof(trans_date),
        "POS_TIME"              , &trans_time                        , "POS_TIME          "   , PTS_DB_TYPE_INT , sizeof(trans_time),
        ""                      , NULL                               , NULL                   ,   0              , 0};
    
    strcpy (sql_info   , "SELECT TRACE.SD_DT_MAX");
    sprintf (sql, 
            "SELECT SHOP_CLEAR_DATE, "
            "       POS_DATE, "
            "       POS_TIME "
            "  FROM %s.A_TRACE "
            " WHERE SYSTEM_REF = %ld "
            , db_posp_name
            , sdtotal_st->max_system_ref
    );
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    //trans_date_long = (long) trans_date;
    //sdtotal_st->trans_datetime_e  =  trans_date_long * 1000000 + trans_time  ;
    sdtotal_st->trans_datetime_e  =  trans_date * 1000000 + trans_time  ;
        
    return iRet;
}


/**
 *获取商户结算账户信息
 **/
int db_get_acct_info(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int  iRet          = -1;
    
    DB_GET_ST data [] = {
        "f_bank_name"                  , sdtotal_st->bank_name        , "结算银行名称"   , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->acct_name),
        "f_bank_union_no"              , sdtotal_st->bank_bid         , "结算联行号  "   , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->bank_bid),
        "f_bank_acct_no"               , sdtotal_st->acct_no          , "结算账户号  "   , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->acct_no),
        "f_bank_acct_name"             , sdtotal_st->acct_name        , "结算账户名  "   , PTS_DB_TYPE_CHAR , sizeof(sdtotal_st->acct_name),
        ""                , NULL        , NULL          ,   0              , 0};
    
    sprintf (sql_info   , "SELECT FILE_ZPSUM.ACCT_INFO_%s", sdtotal_st->shop_no);
    sprintf (sql, 
            "select t.f_bank_name, "
            "       t.f_bank_union_no, "
            "       t.f_bank_acct_no, "
            "       t.f_bank_acct_name "
            "  from t_liquidate_file_zpsum t "
            " where t.f_merchant_no = \'%s\' "
            "   and t.f_file_date = \'%s\' "
            , sdtotal_st->shop_no
            , szDailyDate
    );
    iRet = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
        
    return iRet;
}

/**
 *准备其他字段
 **/
int db_put_sbtotal_other_info(STAT_SDTOTAL_ST *sdtotal_st)
{
    sdtotal_st->cut_date          =  atoi(szDailyDate)    ;
    sdtotal_st->shop_map_id       =  0;  /*暂且0*/

    /////////////////////////////////////////////////////////
    if (atoi(sdtotal_st->shop_clear_type) == SHOP_STL_MODE_FUND)
    {
        sdtotal_st->acct_flag[0]  = '0'; //银联代理划款
    }
    else
    {
        sdtotal_st->acct_flag[0]  = '1'; //未划款
    }
    
    sdtotal_st->pt_settle_fee = 0;
    sdtotal_st->pt_settle_amt = sdtotal_st->pt_shop_net_amt ;

    sdtotal_st->pt_t0_amt      = 0;
    sdtotal_st->pt_t0_fee      = 0;
    sdtotal_st->pt_t0_cnt      = 0;

    sdtotal_st->check_flag[0]  = '0' ; // 待初审
    sdtotal_st->ticket_flag[0] = '1';
    sdtotal_st->acct_time      =  0  ;
    sdtotal_st->user_id1       =  0  ;
    sdtotal_st->user_id2       =  0  ;
    strcpy (sdtotal_st->user_name1 , "-");     // [30 +1]
    strcpy (sdtotal_st->user_name2 , "-");     // [30 +1]
    strcpy (sdtotal_st->acct_desc  , "-");     // [250+1]
    strcpy (sdtotal_st->acct_desc1 , "-");     // [250+1]
    strcpy (sdtotal_st->acct_desc2 , "-");     // [250+1]
    strcpy (sdtotal_st->mac	      , "-");     // [32 +1]
    
    strcpy(sdtotal_st->bank_bid, "-");
    strcpy(sdtotal_st->bank_id, "-");
    sdtotal_st->bank_county_id	      = 0   ;

    strcpy (sdtotal_st->collect_flag	         , "-"	   );
    strcpy (sdtotal_st->acct_type	           , "-"	       );
    strcpy (sdtotal_st->acct_name	           , "-"	       );
    strcpy (sdtotal_st->acct_no	             , "-"	           );
    strcpy (sdtotal_st->bid_name	             , "-"	       );
    strcpy (sdtotal_st->bank_name	           , "-"	       );
    strcpy (sdtotal_st->bank_count_name	     , "-"	   );
    strcpy (sdtotal_st->bank_city_name	       , "-"	   );
    strcpy (sdtotal_st->bank_province_name    , "-" );
    
    return NOERR;
}

/**
 *登记商户日汇总
 **/
int db_insert_sdtotal(MYSQL *conn_ptr, STAT_SDTOTAL_ST *sdtotal_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "BRANCH_MAP_ID"        , &sdtotal_st->branch_map_id	      ,   "BRANCH_MAP_ID      "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->branch_map_id	    ),
        "BRANCH_CODE"          , &sdtotal_st->branch_code	        ,   "BRANCH_CODE        "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->branch_code	        ),
        "PARTNER_MAP_ID"       , &sdtotal_st->partner_map_id	    ,   "PARTNER_MAP_ID     "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->partner_map_id	    ),
        "PARTNER_CODE"         , &sdtotal_st->partner_code	      ,   "PARTNER_CODE       "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->partner_code	        ),
        "SHOP_MAP_ID"          , &sdtotal_st->shop_map_id	        ,   "SHOP_MAP_ID        "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->shop_map_id          ),
        "SHOP_NO"              , &sdtotal_st->shop_no             ,   "SHOP_NO            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->shop_no              ),
        "SHOP_NAMEABCN"        ,  sdtotal_st->shop_nameabcn       ,   "SHOP_NAMEABCN      "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->shop_nameabcn        ),
        "SHOP_NAME"            ,  sdtotal_st->shop_name           ,   "SHOP_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->shop_name            ),
        "CUT_DATE"             , &sdtotal_st->cut_date            ,   "CUT_DATE           "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->cut_date             ),
        "TRANS_DATETIME_B"     , &sdtotal_st->trans_datetime_b    ,   "TRANS_DATETIME_B   "  ,  PTS_DB_TYPE_LONG  , sizeof (sdtotal_st->trans_datetime_b     ),
        "TRANS_DATETIME_E"     , &sdtotal_st->trans_datetime_e    ,   "TRANS_DATETIME_E   "  ,  PTS_DB_TYPE_LONG  , sizeof (sdtotal_st->trans_datetime_e     ),
        "SHOP_CLEAR_DATE"      , &sdtotal_st->shop_clear_date     ,   "SHOP_CLEAR_DATE    "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->shop_clear_date      ),
        "COUNTY_ID"            , &sdtotal_st->county_id	          ,   "COUNTY_ID          "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->county_id            ),
        "SHOP_CLEAR_TYPE"      ,  sdtotal_st->shop_clear_type     ,   "SHOP_CLEAR_TYPE    "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->shop_clear_type      ),
        "PT_TRANS_AMT"         , &sdtotal_st->pt_trans_amt        ,   "PT_TRANS_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_trans_amt         ),
        "PT_SHOP_FEE_AMT"      , &sdtotal_st->pt_shop_fee_amt     ,   "PT_SHOP_FEE_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_shop_fee_amt      ),
        "PT_SHOP_MDR_AMT"      , &sdtotal_st->pt_shop_mdr_amt     ,   "PT_SHOP_MDR_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_shop_mdr_amt      ),
        "PT_SHOP_AVR_AMT"      , &sdtotal_st->pt_shop_avr_amt     ,   "PT_SHOP_AVR_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_shop_avr_amt      ),
        "PT_SHOP_NET_AMT"      , &sdtotal_st->pt_shop_net_amt     ,   "PT_SHOP_NET_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_shop_net_amt      ),
        "PT_MDR_HOST_AMT"      , &sdtotal_st->pt_mdr_host_amt     ,   "PT_MDR_HOST_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_mdr_host_amt      ),
        "PT_MDR_ISS_AMT"       , &sdtotal_st->pt_mdr_iss_amt      ,   "PT_MDR_ISS_AMT     "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_mdr_iss_amt       ),
        "PT_MDR_CUP_AMT"       , &sdtotal_st->pt_mdr_cup_amt      ,   "PT_MDR_CUP_AMT     "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_mdr_cup_amt       ),
        "PT_MDR_LOGO_AMT"      , &sdtotal_st->pt_mdr_logo_amt     ,   "PT_MDR_LOGO_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_mdr_logo_amt      ),
        "PT_MDR_PLAT_AMT"      , &sdtotal_st->pt_mdr_plat_amt     ,   "PT_MDR_PLAT_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_mdr_plat_amt      ),
        "PT_MDR_PARTNER_AMT"   , &sdtotal_st->pt_mdr_partner_amt  ,   "PT_MDR_PARTNER_AMT "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_mdr_partner_amt   ),
        "PT_SETTLE_AMT"        , &sdtotal_st->pt_settle_amt       ,   "PT_SETTLE_AMT      "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_settle_amt        ),
        "PT_SETTLE_FEE"        , &sdtotal_st->pt_settle_fee       ,   "PT_SETTLE_FEE      "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_settle_fee        ),
        "PT_C_CNT"             , &sdtotal_st->pt_c_cnt            ,   "PT_C_CNT           "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->pt_c_cnt             ),
        "PT_C_AMT"             , &sdtotal_st->pt_c_amt            ,   "PT_C_AMT           "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_c_amt             ),
        "PT_D_CNT"             , &sdtotal_st->pt_d_cnt            ,   "PT_D_CNT           "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->pt_d_cnt             ),
        "PT_D_AMT"             , &sdtotal_st->pt_d_amt            ,   "PT_D_AMT           "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_d_amt             ),
        "PT_TOTAL_CNT"         , &sdtotal_st->pt_total_cnt        ,   "PT_TOTAL_CNT       "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->pt_total_cnt         ),
        "PT_TOTAL_AMT"         , &sdtotal_st->pt_total_amt        ,   "PT_TOTAL_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_total_amt         ),
        "PT_ERROR_CNT"         , &sdtotal_st->pt_error_cnt        ,   "PT_ERROR_CNT       "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->pt_error_cnt         ),
        "PT_ERROR_AMT"         , &sdtotal_st->pt_error_amt        ,   "PT_ERROR_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_error_amt         ),
        "PT_AUTO_CNT"          , &sdtotal_st->pt_auto_cnt         ,   "PT_AUTO_CNT        "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->pt_auto_cnt          ),
        "PT_AUTO_AMT"          , &sdtotal_st->pt_auto_amt         ,   "PT_AUTO_AMT        "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_auto_amt          ),
        "PT_T0_AMT"            , &sdtotal_st->pt_t0_amt           ,   "PT_T0_AMT          "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_t0_amt            ),
        "PT_T0_FEE"            , &sdtotal_st->pt_t0_fee           ,   "PT_T0_FEE          "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->pt_t0_fee            ),
        "PT_T0_CNT"            , &sdtotal_st->pt_t0_cnt           ,   "PT_T0_CNT          "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->pt_t0_cnt            ),
        "CHECK_FLAG"           ,  sdtotal_st->check_flag          ,   "CHECK_FLAG         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->check_flag           ),
        "ACCT_FLAG"            ,  sdtotal_st->acct_flag           ,   "ACCT_FLAG          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->acct_flag            ),
        "ACCT_TIME"            , &sdtotal_st->acct_time           ,   "ACCT_TIME          "  ,  PTS_DB_TYPE_LONG , sizeof (sdtotal_st->acct_time            ),
        "TICKET_FLAG"          ,  sdtotal_st->ticket_flag         ,   "TICKET_FLAG        "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->ticket_flag          ),
        "USER_ID1"             , &sdtotal_st->user_id1            ,   "USER_ID1           "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->user_id1             ),
        "USER_NAME1"           ,  sdtotal_st->user_name1          ,   "USER_NAME1         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->user_name1           ),
        "USER_ID2"             , &sdtotal_st->user_id2            ,   "USER_ID2           "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->user_id2             ),
        "USER_NAME2"           ,  sdtotal_st->user_name2          ,   "USER_NAME2         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->user_name2           ),
        "ACCT_DESC"            ,  sdtotal_st->acct_desc           ,   "ACCT_DESC          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->acct_desc            ),
        "ACCT_DESC1"           ,  sdtotal_st->acct_desc1          ,   "ACCT_DESC1         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->acct_desc1           ),
        "ACCT_DESC2"           ,  sdtotal_st->acct_desc2          ,   "ACCT_DESC2         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->acct_desc2           ),
        "MAC"                  ,  sdtotal_st->mac                 ,   "MAC                "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->mac                  ),
        "COLLECT_FLAG"         ,  sdtotal_st->collect_flag	      ,   "COLLECT_FLAG       "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->collect_flag	        ),
        "ACCT_TYPE"            ,  sdtotal_st->acct_type	          ,   "ACCT_TYPE          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->acct_type	        ),
        "ACCT_NAME"            ,  sdtotal_st->acct_name	          ,   "ACCT_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->acct_name	        ),
        "ACCT_NO"              ,  sdtotal_st->acct_no	            ,   "ACCT_NO            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->acct_no	            ),
        "BANK_BID"             ,  sdtotal_st->bank_bid	          ,   "BANK_BID           "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->bank_bid	            ),
        "BID_NAME"             ,  sdtotal_st->bid_name	          ,   "BID_NAME           "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->bid_name	            ),
        "BANK_ID"              ,  sdtotal_st->bank_id	            ,   "BANK_ID            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->bank_id	            ),
        "BANK_NAME"            ,  sdtotal_st->bank_name	          ,   "BANK_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->bank_name	        ),
        "BANK_COUNTY_ID"       , &sdtotal_st->bank_county_id	    ,   "BANK_COUNTY_ID     "  ,  PTS_DB_TYPE_INT  , sizeof (sdtotal_st->bank_county_id	    ),
        "BANK_COUNT_NAME"      ,  sdtotal_st->bank_count_name	    ,   "BANK_COUNT_NAME    "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->bank_count_name	    ),
        "BANK_CITY_NAME"       ,  sdtotal_st->bank_city_name	    ,   "BANK_CITY_NAME     "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->bank_city_name	    ),
        "BANK_PROVINCE_NAME"   ,  sdtotal_st->bank_province_name  ,   "BANK_PROVINCE_NAME "  ,  PTS_DB_TYPE_CHAR , sizeof (sdtotal_st->bank_province_name   ),
        ""                     ,  NULL                            ,   NULL                   ,  0                ,   0 };
        
    strcpy (sql_info   , "INSERT A_SDTOTAL");
    sprintf(sql        , "INSERT INTO %s.A_SDTOTAL SET ", db_posp_name);
    
    iRet = db_insert_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    return (iRet);
}

/**
 *统计汇总
 **/
int func_merchant_stat_sum(MYSQL *conn_ptr)
{
    int  i, iRet;
    char sql[2048]    = { 0 };
    char sql_info[512]= { 0 };
    STAT_SDTOTAL_ST sdtotal_st;
    DB_RESULT       *db_result = NULL;
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    //1. 按照商户地区区间,商户ID分组，获取交易金额汇总
    sprintf (sql_info   , "SELECT TRACE_SDBILL");
    sprintf (sql,
             "SELECT SHOP_NO, "
             "       SHOP_NAMEAB AS SHOP_NAME, "
             "       SHOP_NAMEAB AS SHOP_NAMEAB, "
             "       COUNTY_ID, "
             "       BRANCH_MAP_ID,"
             "       BRANCH_CODE,"
             "       PARTNER_MAP_ID,"
             "       PARTNER_CODE,"
             "       MAX(SYSTEM_REF) AS MAX_SYSTEM_REF, "
             "       MIN(SYSTEM_REF) AS MIN_SYSTEM_REF, "
             "       SUM(TRANS_AMT) AS PT_TRANS_AMT, "
             "       SUM(SHOP_FEE_AMT) AS PT_SHOP_FEE_AMT, "
             "       SUM(SHOP_MDR_AMT) AS PT_SHOP_MDR_AMT, "
             "       SUM(SHOP_AVR_AMT) AS PT_SHOP_AVR_AMT, "
             "       SUM(SHOP_NET_AMT) AS PT_SHOP_NET_AMT, "
             "       SUM(MDR_HOST_AMT) AS PT_MDR_HOST_AMT, "
             "       SUM(MDR_ISS_AMT) AS PT_MDR_ISS_AMT, "
             "       SUM(MDR_CUP_AMT) AS PT_MDR_CUP_AMT, "
             "       SUM(MDR_LOGO_AMT) AS PT_MDR_LOGO_AMT, "
             "       SUM(MDR_PLAT_AMT) AS PT_MDR_PLAT_AMT, "
             "       SUM(MDR_PARTNER_AMT) AS PT_MDR_PARTNER_AMT, "
             "       SHOP_CLEAR_TYPE "
             "  FROM %s.A_TRACE "
             " WHERE CUT_DATE = \'%s\' "
             "   AND TRANS_RETCODE = \'00\' "
             "   AND TRANS_STATUS = \'0\' "
             "   AND TRANS_REVERFLAG = \'0\' "
             "   AND TRANS_AMT != \'0\' "
             "   AND HOST_MAP_ID = %d "
             " GROUP BY SHOP_NO, "
             "          SHOP_NAMEAB, "
             "          COUNTY_ID, "
             "          BRANCH_MAP_ID, "
             "          BRANCH_CODE, "
             "          PARTNER_MAP_ID, "
             "          PARTNER_CODE, "
             "          SHOP_CLEAR_TYPE "
             " ORDER BY SHOP_NO"
             , db_posp_name
             , szDailyDate
             , CHANNEL_CUP_DIRECT
    );
    
    /*执行游标*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "需要处理的流水笔数[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        /*获取流水数据*/
        memset (&sdtotal_st, 0, sizeof(STAT_SDTOTAL_ST));
        iRet = db_get_shop_county_info(db_result, &sdtotal_st, sql_info);
        if (iRet == -2)
        {
            SysLog( LOGTYPE_INFO , "遍历结束" );
            break;
        }

        /*系统流水重复校验*/
        iRet = db_select_exist_sdtotal(conn_ptr, &sdtotal_st);
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
            SysLog(LOGTYPE_INFO, "商户[%s] 日切日期[%s].已经日终生成日汇总完成.WARNNING!", sdtotal_st.shop_no, szDailyDate);
            continue;
        }
        
        /*消费成功统计*/
        iRet = db_stat_consum_succ(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*退货成功统计*/
        iRet = db_stat_refund_succ(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*所有交易统计*/
        iRet = db_stat_all_type(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*所有失败交易统计*/
        db_stat_all_fail(&sdtotal_st);
        
        /*所有冲正交易统计*/
        iRet = db_stat_all_reve(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        /*交易日期时间的范围-开始*/
        iRet = db_get_time_begin(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        /*交易日期时间的范围-结束*/
        iRet = db_get_time_end(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*交易日期时间的范围-结束*/
        iRet = db_put_sbtotal_other_info(&sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*获取结算账户信息*/
        iRet = db_get_acct_info(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*登记商户日汇总*/
        iRet = db_insert_sdtotal(conn_ptr, &sdtotal_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        db_commit(conn_ptr);
        
        SysLog( LOGTYPE_DEBUG , "NO.[%d].商户[%s] 日切日期[%s].日终生成日汇总单.生成成功,SUCC!", i+1, sdtotal_st.shop_no, szDailyDate);
    }
    
    db_free_result(db_result);
    
    return NOERR;
}

int db_get_shop_total(DB_RESULT * db_result, STAT_SDBILL_ST *sdbill_st, char *sql_info)
{
    int iRet;
    char sql[1024]     = { 0 };
    long pt_c_cnt, pt_d_cnt, pt_total_cnt, pt_error_cnt, pt_auto_cnt, pt_t0_cnt;
    
    DB_GET_ST data [] = {
        "BRANCH_MAP_ID"        , &sdbill_st->branch_map_id	    ,   "BRANCH_MAP_ID      "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->branch_map_id	    ),
        "BRANCH_CODE"          , &sdbill_st->branch_code	        ,   "BRANCH_CODE        "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->branch_code	        ),
        "PARTNER_MAP_ID"       , &sdbill_st->partner_map_id	    ,   "PARTNER_MAP_ID     "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->partner_map_id	    ),
        "PARTNER_CODE"         , &sdbill_st->partner_code	    ,   "PARTNER_CODE       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->partner_code	        ),
        "SHOP_MAP_ID"          , &sdbill_st->shop_map_id	        ,   "SHOP_MAP_ID        "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->shop_map_id          ),
        "SHOP_NO"              , &sdbill_st->shop_no             ,   "SHOP_NO            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_no              ),
        "SHOP_NAMEABCN"        ,  sdbill_st->shop_nameabcn       ,   "SHOP_NAMEABCN      "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_nameabcn        ),
        "SHOP_NAME"            ,  sdbill_st->shop_name           ,   "SHOP_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_name            ),
        "CUT_DATE"             , &sdbill_st->cut_date            ,   "CUT_DATE           "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->cut_date             ),
        "MIN_TRANS_DATETIME_B" , &sdbill_st->trans_datetime_b    ,   "MIN_TRANS_DATETIME_B   "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->trans_datetime_b     ),
        "MAX_TRANS_DATETIME_E" , &sdbill_st->trans_datetime_e    ,   "MAX_TRANS_DATETIME_E   "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->trans_datetime_e     ),
        "SHOP_CLEAR_DATE"      , &sdbill_st->shop_clear_date     ,   "SHOP_CLEAR_DATE    "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->shop_clear_date      ),
        "COUNTY_ID"            , &sdbill_st->county_id	        ,   "COUNTY_ID          "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->county_id            ),
        "SHOP_CLEAR_TYPE"      ,  sdbill_st->shop_clear_type     ,   "SHOP_CLEAR_TYPE    "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_clear_type      ),
        "PT_TRANS_AMT"         , &sdbill_st->pt_trans_amt        ,   "PT_TRANS_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_trans_amt         ),
        "PT_SHOP_FEE_AMT"      , &sdbill_st->pt_shop_fee_amt     ,   "PT_SHOP_FEE_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_fee_amt      ),
        "PT_SHOP_MDR_AMT"      , &sdbill_st->pt_shop_mdr_amt     ,   "PT_SHOP_MDR_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_mdr_amt      ),
        "PT_SHOP_AVR_AMT"      , &sdbill_st->pt_shop_avr_amt     ,   "PT_SHOP_AVR_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_avr_amt      ),
        "PT_SHOP_NET_AMT"      , &sdbill_st->pt_shop_net_amt     ,   "PT_SHOP_NET_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_net_amt      ),
        "PT_MDR_HOST_AMT"      , &sdbill_st->pt_mdr_host_amt     ,   "PT_MDR_HOST_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_host_amt      ),
        "PT_MDR_ISS_AMT"       , &sdbill_st->pt_mdr_iss_amt      ,   "PT_MDR_ISS_AMT     "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_iss_amt       ),
        "PT_MDR_CUP_AMT"       , &sdbill_st->pt_mdr_cup_amt      ,   "PT_MDR_CUP_AMT     "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_cup_amt       ),
        "PT_MDR_LOGO_AMT"      , &sdbill_st->pt_mdr_logo_amt     ,   "PT_MDR_LOGO_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_logo_amt      ),
        "PT_MDR_PLAT_AMT"      , &sdbill_st->pt_mdr_plat_amt     ,   "PT_MDR_PLAT_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_plat_amt      ),
        "PT_MDR_PARTNER_AMT"   , &sdbill_st->pt_mdr_partner_amt  ,   "PT_MDR_PARTNER_AMT "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_partner_amt   ),
        "PT_SETTLE_AMT"        , &sdbill_st->pt_settle_amt       ,   "PT_SETTLE_AMT      "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_settle_amt        ),
        "PT_SETTLE_FEE"        , &sdbill_st->pt_settle_fee       ,   "PT_SETTLE_FEE      "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_settle_fee        ),
        "PT_C_CNT"             , &pt_c_cnt                       ,   "PT_C_CNT           "  ,  PTS_DB_TYPE_LONG , sizeof (pt_c_cnt             ),
        "PT_C_AMT"             , &sdbill_st->pt_c_amt            ,   "PT_C_AMT           "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_c_amt             ),
        "PT_D_CNT"             , &pt_d_cnt                       ,   "PT_D_CNT           "  ,  PTS_DB_TYPE_LONG , sizeof (pt_d_cnt             ),
        "PT_D_AMT"             , &sdbill_st->pt_d_amt            ,   "PT_D_AMT           "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_d_amt             ),
        "PT_TOTAL_CNT"         , &pt_total_cnt                   ,   "PT_TOTAL_CNT       "  ,  PTS_DB_TYPE_LONG , sizeof (pt_total_cnt         ),
        "PT_TOTAL_AMT"         , &sdbill_st->pt_total_amt        ,   "PT_TOTAL_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_total_amt         ),
        "PT_ERROR_CNT"         , &pt_error_cnt                   ,   "PT_ERROR_CNT       "  ,  PTS_DB_TYPE_LONG , sizeof (pt_error_cnt         ),
        "PT_ERROR_AMT"         , &sdbill_st->pt_error_amt        ,   "PT_ERROR_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_error_amt         ),
        "PT_AUTO_CNT"          , &pt_auto_cnt                    ,   "PT_AUTO_CNT        "  ,  PTS_DB_TYPE_LONG , sizeof (pt_auto_cnt          ),
        "PT_AUTO_AMT"          , &sdbill_st->pt_auto_amt         ,   "PT_AUTO_AMT        "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_auto_amt          ),
        "PT_T0_AMT"            , &sdbill_st->pt_t0_amt           ,   "PT_T0_AMT          "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_t0_amt            ),
        "PT_T0_FEE"            , &sdbill_st->pt_t0_fee           ,   "PT_T0_FEE          "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_t0_fee            ),
        "PT_T0_CNT"            , &pt_t0_cnt                      ,   "PT_T0_CNT          "  ,  PTS_DB_TYPE_LONG , sizeof (pt_t0_cnt            ),
        "ACCT_FLAG"            ,  sdbill_st->acct_flag           ,   "ACCT_FLAG          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_flag            ),
        "TICKET_FLAG"          ,  sdbill_st->ticket_flag         ,   "TICKET_FLAG        "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->ticket_flag          ),
        "COLLECT_FLAG"         ,  sdbill_st->collect_flag	       ,   "COLLECT_FLAG       "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->collect_flag	        ),
        "ACCT_TYPE"            ,  sdbill_st->acct_type	         ,   "ACCT_TYPE          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_type	        ),
        "ACCT_NAME"            ,  sdbill_st->acct_name	         ,   "ACCT_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_name	        ),
        "ACCT_NO"              ,  sdbill_st->acct_no	           ,   "ACCT_NO            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_no	            ),
        "BANK_BID"             ,  sdbill_st->bank_bid	           ,   "BANK_BID           "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_bid	            ),
        "BID_NAME"             ,  sdbill_st->bid_name	           ,   "BID_NAME           "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bid_name	            ),
        "BANK_ID"              ,  sdbill_st->bank_id	           ,   "BANK_ID            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_id	            ),
        "BANK_NAME"            ,  sdbill_st->bank_name	         ,   "BANK_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_name	        ),
        "BANK_COUNTY_ID"       , &sdbill_st->bank_county_id	     ,   "BANK_COUNTY_ID     "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->bank_county_id	    ),
        "BANK_COUNT_NAME"      ,  sdbill_st->bank_count_name	   ,   "BANK_COUNT_NAME    "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_count_name	    ),
        "BANK_CITY_NAME"       ,  sdbill_st->bank_city_name	     ,   "BANK_CITY_NAME     "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_city_name	    ),
        "BANK_PROVINCE_NAME"   ,  sdbill_st->bank_province_name  ,   "BANK_PROVINCE_NAME "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_province_name   ),
        ""                     ,  NULL                           ,   NULL                   ,  0                ,   0 };
    
    iRet = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);
    
    /*转换数据类型*/
    sdbill_st->pt_c_cnt = (int) pt_c_cnt;
    sdbill_st->pt_d_cnt = (int) pt_d_cnt;
    sdbill_st->pt_total_cnt = (int) pt_total_cnt;
    sdbill_st->pt_error_cnt = (int) pt_error_cnt;
    sdbill_st->pt_auto_cnt = (int) pt_auto_cnt;
    sdbill_st->pt_t0_cnt = (int) pt_t0_cnt;
    
    return iRet;
}

/**
 *准备结算单其他字段
 **/
int db_put_sdbill_other_info(STAT_SDBILL_ST *sdbill_st)
{
    sdbill_st->ticket_flag[0] = '1' ;
    sdbill_st->acct_time      =  0  ;
    sdbill_st->user_id1       =  0  ;
    sdbill_st->user_id2       =  0  ;
    strcpy (sdbill_st->user_name1 , "-");     // [30 +1]
    strcpy (sdbill_st->user_name2 , "-");     // [30 +1]
    strcpy (sdbill_st->acct_desc  , "-");     // [250+1]
    strcpy (sdbill_st->acct_desc1 , "-");     // [250+1]
    strcpy (sdbill_st->acct_desc2 , "-");     // [250+1]
    strcpy (sdbill_st->mac	     , "-");     // [32 +1]
    
    strcpy (sdbill_st->collect_flag	       , "-"	   );
    
    return NOERR;
}


/**
 *检测是否已经登记
 **/
int db_select_exist_sdbill(MYSQL *conn_ptr, STAT_SDBILL_ST *sdbill_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;

    sprintf (sql_info   , "SELECT SDBILL.EXSIT_%s", sdbill_st->shop_no);
    sprintf (sql, 
            "SELECT SHOP_MAP_ID "
            "  FROM %s.A_SDBILL "
            " WHERE SHOP_NO = \'%s\' "
            "   AND SHOP_CLEAR_DATE = \'%s\'"
            , db_posp_name
            , sdbill_st->shop_no
            , szDailyDate
    );

    iRet = db_exsit_check (conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    return iRet;
}

/**
 *登记商户日汇总
 **/
int db_insert_sdbill(MYSQL *conn_ptr, STAT_SDBILL_ST *sdbill_st)
{
    char sql[2048]     = { 0 };
    char sql_info[512] = { 0 };
    int    iRet          = -1;
    
    DB_GET_ST data [] = {
        "BRANCH_MAP_ID"        , &sdbill_st->branch_map_id	     ,   "BRANCH_MAP_ID      "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->branch_map_id	    ),
        "BRANCH_CODE"          , &sdbill_st->branch_code	       ,   "BRANCH_CODE        "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->branch_code	        ),
        "PARTNER_MAP_ID"       , &sdbill_st->partner_map_id	     ,   "PARTNER_MAP_ID     "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->partner_map_id	    ),
        "PARTNER_CODE"         , &sdbill_st->partner_code	       ,   "PARTNER_CODE       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->partner_code	        ),
        "SHOP_MAP_ID"          , &sdbill_st->shop_map_id	       ,   "SHOP_MAP_ID        "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->shop_map_id          ),
        "SHOP_NO"              , &sdbill_st->shop_no             ,   "SHOP_NO            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_no              ),
        "SHOP_NAMEABCN"        ,  sdbill_st->shop_nameabcn       ,   "SHOP_NAMEABCN      "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_nameabcn        ),
        "SHOP_NAME"            ,  sdbill_st->shop_name           ,   "SHOP_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_name            ),
        "CUT_DATE"             , &sdbill_st->cut_date            ,   "CUT_DATE           "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->cut_date             ),
        "TRANS_DATETIME_B"     , &sdbill_st->trans_datetime_b    ,   "TRANS_DATETIME_B   "  ,  PTS_DB_TYPE_LONG  , sizeof (sdbill_st->trans_datetime_b     ),
        "TRANS_DATETIME_E"     , &sdbill_st->trans_datetime_e    ,   "TRANS_DATETIME_E   "  ,  PTS_DB_TYPE_LONG  , sizeof (sdbill_st->trans_datetime_e     ),
        "SHOP_CLEAR_DATE"      , &sdbill_st->shop_clear_date     ,   "SHOP_CLEAR_DATE    "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->shop_clear_date      ),
        "COUNTY_ID"            , &sdbill_st->county_id	        ,   "COUNTY_ID           "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->county_id            ),
        "SHOP_CLEAR_TYPE"      ,  sdbill_st->shop_clear_type     ,   "SHOP_CLEAR_TYPE    "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->shop_clear_type      ),
        "PT_TRANS_AMT"         , &sdbill_st->pt_trans_amt        ,   "PT_TRANS_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_trans_amt         ),
        "PT_SHOP_FEE_AMT"      , &sdbill_st->pt_shop_fee_amt     ,   "PT_SHOP_FEE_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_fee_amt      ),
        "PT_SHOP_MDR_AMT"      , &sdbill_st->pt_shop_mdr_amt     ,   "PT_SHOP_MDR_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_mdr_amt      ),
        "PT_SHOP_AVR_AMT"      , &sdbill_st->pt_shop_avr_amt     ,   "PT_SHOP_AVR_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_avr_amt      ),
        "PT_SHOP_NET_AMT"      , &sdbill_st->pt_shop_net_amt     ,   "PT_SHOP_NET_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_shop_net_amt      ),
        "PT_MDR_HOST_AMT"      , &sdbill_st->pt_mdr_host_amt     ,   "PT_MDR_HOST_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_host_amt      ),
        "PT_MDR_ISS_AMT"       , &sdbill_st->pt_mdr_iss_amt      ,   "PT_MDR_ISS_AMT     "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_iss_amt       ),
        "PT_MDR_CUP_AMT"       , &sdbill_st->pt_mdr_cup_amt      ,   "PT_MDR_CUP_AMT     "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_cup_amt       ),
        "PT_MDR_LOGO_AMT"      , &sdbill_st->pt_mdr_logo_amt     ,   "PT_MDR_LOGO_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_logo_amt      ),
        "PT_MDR_PLAT_AMT"      , &sdbill_st->pt_mdr_plat_amt     ,   "PT_MDR_PLAT_AMT    "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_plat_amt      ),
        "PT_MDR_PARTNER_AMT"   , &sdbill_st->pt_mdr_partner_amt  ,   "PT_MDR_PARTNER_AMT "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_mdr_partner_amt   ),
        "PT_SETTLE_AMT"        , &sdbill_st->pt_settle_amt       ,   "PT_SETTLE_AMT      "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_settle_amt        ),
        "PT_SETTLE_FEE"        , &sdbill_st->pt_settle_fee       ,   "PT_SETTLE_FEE      "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_settle_fee        ),
        "PT_C_CNT"             , &sdbill_st->pt_c_cnt            ,   "PT_C_CNT           "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->pt_c_cnt             ),
        "PT_C_AMT"             , &sdbill_st->pt_c_amt            ,   "PT_C_AMT           "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_c_amt             ),
        "PT_D_CNT"             , &sdbill_st->pt_d_cnt            ,   "PT_D_CNT           "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->pt_d_cnt             ),
        "PT_D_AMT"             , &sdbill_st->pt_d_amt            ,   "PT_D_AMT           "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_d_amt             ),
        "PT_TOTAL_CNT"         , &sdbill_st->pt_total_cnt        ,   "PT_TOTAL_CNT       "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->pt_total_cnt         ),
        "PT_TOTAL_AMT"         , &sdbill_st->pt_total_amt        ,   "PT_TOTAL_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_total_amt         ),
        "PT_ERROR_CNT"         , &sdbill_st->pt_error_cnt        ,   "PT_ERROR_CNT       "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->pt_error_cnt         ),
        "PT_ERROR_AMT"         , &sdbill_st->pt_error_amt        ,   "PT_ERROR_AMT       "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_error_amt         ),
        "PT_AUTO_CNT"          , &sdbill_st->pt_auto_cnt         ,   "PT_AUTO_CNT        "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->pt_auto_cnt          ),
        "PT_AUTO_AMT"          , &sdbill_st->pt_auto_amt         ,   "PT_AUTO_AMT        "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_auto_amt          ),
        "PT_T0_AMT"            , &sdbill_st->pt_t0_amt           ,   "PT_T0_AMT          "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_t0_amt            ),
        "PT_T0_FEE"            , &sdbill_st->pt_t0_fee           ,   "PT_T0_FEE          "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->pt_t0_fee            ),
        "PT_T0_CNT"            , &sdbill_st->pt_t0_cnt           ,   "PT_T0_CNT          "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->pt_t0_cnt            ),
        "CHECK_FLAG"           ,  sdbill_st->check_flag          ,   "CHECK_FLAG         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->check_flag           ),
        "ACCT_FLAG"            ,  sdbill_st->acct_flag           ,   "ACCT_FLAG          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_flag            ),
        "ACCT_TIME"            , &sdbill_st->acct_time           ,   "ACCT_TIME          "  ,  PTS_DB_TYPE_LONG , sizeof (sdbill_st->acct_time            ),
        "TICKET_FLAG"          ,  sdbill_st->ticket_flag         ,   "TICKET_FLAG        "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->ticket_flag          ),
        "USER_ID1"             , &sdbill_st->user_id1            ,   "USER_ID1           "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->user_id1             ),
        "USER_NAME1"           ,  sdbill_st->user_name1          ,   "USER_NAME1         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->user_name1           ),
        "USER_ID2"             , &sdbill_st->user_id2            ,   "USER_ID2           "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->user_id2             ),
        "USER_NAME2"           ,  sdbill_st->user_name2          ,   "USER_NAME2         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->user_name2           ),
        "ACCT_DESC"            ,  sdbill_st->acct_desc           ,   "ACCT_DESC          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_desc            ),
        "ACCT_DESC1"           ,  sdbill_st->acct_desc1          ,   "ACCT_DESC1         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_desc1           ),
        "ACCT_DESC2"           ,  sdbill_st->acct_desc2          ,   "ACCT_DESC2         "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_desc2           ),
        "MAC"                  ,  sdbill_st->mac                 ,   "MAC                "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->mac                  ),
        "COLLECT_FLAG"         ,  sdbill_st->collect_flag	       ,   "COLLECT_FLAG       "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->collect_flag	        ),
        "ACCT_TYPE"            ,  sdbill_st->acct_type	         ,   "ACCT_TYPE          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_type	        ),
        "ACCT_NAME"            ,  sdbill_st->acct_name	         ,   "ACCT_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_name	        ),
        "ACCT_NO"              ,  sdbill_st->acct_no	           ,   "ACCT_NO            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->acct_no	            ),
        "BANK_BID"             ,  sdbill_st->bank_bid	           ,   "BANK_BID           "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_bid	            ),
        "BID_NAME"             ,  sdbill_st->bid_name	           ,   "BID_NAME           "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bid_name	            ),
        "BANK_ID"              ,  sdbill_st->bank_id	           ,   "BANK_ID            "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_id	            ),
        "BANK_NAME"            ,  sdbill_st->bank_name	         ,   "BANK_NAME          "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_name	        ),
        "BANK_COUNTY_ID"       , &sdbill_st->bank_county_id	     ,   "BANK_COUNTY_ID     "  ,  PTS_DB_TYPE_INT  , sizeof (sdbill_st->bank_county_id	    ),
        "BANK_COUNT_NAME"      ,  sdbill_st->bank_count_name	   ,   "BANK_COUNT_NAME    "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_count_name	    ),
        "BANK_CITY_NAME"       ,  sdbill_st->bank_city_name	     ,   "BANK_CITY_NAME     "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_city_name	    ),
        "BANK_PROVINCE_NAME"   ,  sdbill_st->bank_province_name  ,   "BANK_PROVINCE_NAME "  ,  PTS_DB_TYPE_CHAR , sizeof (sdbill_st->bank_province_name   ),
        ""                     ,  NULL                            ,   NULL                   ,  0                ,   0 };
        
    strcpy (sql_info   , "INSERT A_SDBILL");
    sprintf(sql        , "INSERT INTO %s.A_SDBILL SET ", db_posp_name);
    
    iRet = db_insert_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    
    return (iRet);
}

/**
 *统计结算单
 **/
int func_merchant_stat_settle(MYSQL *conn_ptr)
{
    int  i, iRet;
    char sql[2048]    = { 0 };
    char sql_info[512]= { 0 };
    STAT_SDBILL_ST  sdbill_st;
    DB_RESULT       *db_result = NULL;
    
    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));
    
    sprintf (sql_info   , "SELECT A_SDTOTAL");
    sprintf (sql,
             "SELECT SHOP_MAP_ID, "
                   " SHOP_NO, "
                   " CUT_DATE, "
                   " MAX(TRANS_DATETIME_B) AS MIN_TRANS_DATETIME_B, "
                   " MIN(TRANS_DATETIME_E) AS MAX_TRANS_DATETIME_E, "
                   " SHOP_CLEAR_DATE,"
                   " SHOP_NAMEABCN AS SHOP_NAMEABCN, "
                   " SHOP_NAME AS SHOP_NAME, "
                   " BRANCH_MAP_ID,"
                   " BRANCH_CODE,"
                   " PARTNER_MAP_ID,"
                   " PARTNER_CODE,"
                   " SHOP_CLEAR_TYPE, "
                   " COUNTY_ID, "
                   " SUM(PT_TRANS_AMT) AS PT_TRANS_AMT, "
                   " SUM(PT_SHOP_FEE_AMT) AS PT_SHOP_FEE_AMT, "
                   " SUM(PT_SHOP_MDR_AMT) AS PT_SHOP_MDR_AMT, "
                   " SUM(PT_SHOP_AVR_AMT) AS PT_SHOP_AVR_AMT, "
                   " SUM(PT_SHOP_NET_AMT) AS PT_SHOP_NET_AMT, "
                   " SUM(PT_MDR_HOST_AMT) AS PT_MDR_HOST_AMT, "
                   " SUM(PT_MDR_ISS_AMT) AS PT_MDR_ISS_AMT, "
                   " SUM(PT_MDR_CUP_AMT) AS PT_MDR_CUP_AMT, "
                   " SUM(PT_MDR_LOGO_AMT) AS PT_MDR_LOGO_AMT, "
                   " SUM(PT_MDR_PLAT_AMT) AS PT_MDR_PLAT_AMT, "
                   " SUM(PT_MDR_PARTNER_AMT) AS PT_MDR_PARTNER_AMT, "
                   " SUM(PT_SETTLE_AMT) AS PT_SETTLE_AMT, "
                   " SUM(PT_SETTLE_FEE) AS PT_SETTLE_FEE, "
                   " SUM(PT_C_CNT) AS PT_C_CNT, "
                   " SUM(PT_C_AMT) AS PT_C_AMT, "
                   " SUM(PT_D_CNT) AS PT_D_CNT, "
                   " SUM(PT_D_AMT) AS PT_D_AMT, "
                   " SUM(PT_TOTAL_CNT) AS PT_TOTAL_CNT, "
                   " SUM(PT_TOTAL_AMT) AS PT_TOTAL_AMT, "
                   " SUM(PT_ERROR_CNT) AS PT_ERROR_CNT, "
                   " SUM(PT_ERROR_AMT) AS PT_ERROR_AMT, "
                   " SUM(PT_AUTO_CNT) AS PT_AUTO_CNT, "
                   " SUM(PT_AUTO_AMT) AS PT_AUTO_AMT, "
                   " SUM(PT_T0_AMT) AS PT_T0_AMT, "
                   " SUM(PT_T0_FEE) AS PT_T0_FEE, "
                   " SUM(PT_AUTO_AMT) AS PT_T0_CNT, "
                   " TICKET_FLAG, "
                   " COLLECT_FLAG, "
                   " ACCT_FLAG, "
                   " ACCT_TYPE, "
                   " ACCT_NAME, "
                   " ACCT_NO, "
                   " BANK_BID, "
                   " BID_NAME, "
                   " BANK_ID, "
                   " BANK_NAME, "
                   " BANK_COUNTY_ID, "
                   " BANK_COUNT_NAME, "
                   " BANK_CITY_NAME, "
                   " BANK_PROVINCE_NAME "
             "  FROM %s.A_SDTOTAL "
             " WHERE SHOP_CLEAR_DATE = \'%s\' "
             //"   AND HOST_MAP_ID = %d "
             " GROUP BY SHOP_MAP_ID, "
                   " SHOP_NO, "
                   " CUT_DATE, "
                   " SHOP_CLEAR_DATE,"
                   " SHOP_NAMEABCN, "
                   " SHOP_NAME, "
                   " BRANCH_MAP_ID,"
                   " BRANCH_CODE,"
                   " PARTNER_MAP_ID,"
                   " PARTNER_CODE,"
                   " SHOP_CLEAR_TYPE, "
                   " COUNTY_ID, "
                   " TICKET_FLAG, "
                   " COLLECT_FLAG, "
                   " ACCT_FLAG, "
                   " ACCT_TYPE, "
                   " ACCT_NAME, "
                   " ACCT_NO, "
                   " BANK_BID, "
                   " BID_NAME, "
                   " BANK_ID, "
                   " BANK_NAME, "
                   " BANK_COUNTY_ID, "
                   " BANK_COUNT_NAME, "
                   " BANK_CITY_NAME, "
                   " BANK_PROVINCE_NAME "
             " ORDER BY SHOP_NO "
             , db_posp_name
             , szDailyDate
             //, CHANNEL_CUP_DIRECT
    );

    /*执行游标*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "需要处理的流水笔数[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        /*获取流水数据*/
        memset (&sdbill_st, 0, sizeof(STAT_SDBILL_ST));
        iRet = db_get_shop_total(db_result, &sdbill_st, sql_info);
        if (iRet == -2)
        {
            SysLog( LOGTYPE_INFO , "遍历结束" );
            break;
        }

        /*系统流水重复校验*/
        iRet = db_select_exist_sdbill(conn_ptr, &sdbill_st);
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
            SysLog(LOGTYPE_INFO, "商户[%s] 日切日期[%s].已经日终生成结算安完成.WARNNING!", sdbill_st.shop_no, szDailyDate);
            continue;
        }
        
        /*交易日期时间的范围-结束*/
        iRet = db_put_sdbill_other_info(&sdbill_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }
        
        /*登记商户结算单*/
        iRet = db_insert_sdbill(conn_ptr, &sdbill_st);
        if (iRet < 0 && iRet != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (iRet);
        }

        db_commit(conn_ptr);
        
        SysLog( LOGTYPE_DEBUG , "NO.[%d].商户[%s] 日切日期[%s].日终生成结算单.生成成功,SUCC!", i+1, sdbill_st.shop_no, szDailyDate);
    }
    
    db_free_result(db_result);
    
    return NOERR;
}

/**
 *商户数据统计
 **/
int func_merchant_stat_exec(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    iRet = func_merchant_stat_sum(conn_ptr);
    if ( NOERR != iRet )
    {
        return ERROR;
    }
    
    iRet = func_merchant_stat_settle(conn_ptr);
    if ( NOERR != iRet )
    {
        return ERROR;
    }
    
    return NOERR;
}

/**
 *结束
 **/
int func_merchant_stat_end(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "商户数据统计结束……" );
    
    return NOERR;
}

/**
 *入口函数
 **/
int func_merchant_stat(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    /*登记流水初始化*/
    iRet = func_merchant_stat_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "商户数据统计初始化失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*登记流水*/
    iRet = func_merchant_stat_exec(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "商户数据统计失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*登记流水结束*/
    iRet = func_merchant_stat_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "商户数据统计结束失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    return STEP_EXEC_STATUS_SUCC;
}