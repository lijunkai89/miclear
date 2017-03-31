/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 清分清算系统
 *    SystemName  : 调度步骤-登记直连商户交易流水到流水表
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-登记直连商户交易流水到流水表
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/03/09         北京           李君凯         创建文档
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"
#include "dbbase.h"

/*对账最大差异日*/
#define MAX_DEFF_DAYS 2

#define LOG_SQL_TITLE "登记系统流水"

static char  szDailyDate[9]    = { 0 };
static char  db_posp_name[30]  = { 0 };

typedef struct {
    long f_id;
    char f_file_date[8+1];
    int  f_channel_no;
    char f_agent_code[11+1];
    char f_snd_code[11+1];
    char f_rcv_code[11+1];
    char f_iss_code[11+1];
    char f_trace_no[6+1];
    char f_tran_time[10+1];
    char f_card_no[19+1];
    long f_tran_amt;
    int f_tran_type;
    char f_mcc[4+1];
    char f_merchant_no[15+1];
    char f_terminal_no[8+1];
    char f_tran_rrn[12+1];
    char f_auth_no[6+1];
    char f_msg_code[4+1];
    char f_tran_code[6+1];
    char f_sub_tran_code[2+1];
    char f_resp_code[2+1];
    char f_pos_mode[4+1];
    long f_rcv_fee;
    long f_pay_fee;
    long f_tran_fee;
    char f_card_sn[3+1];
    char f_org_trace_no[6+1];
    char f_org_tran_time[10+1];
    char f_data_acoma[1024+1];
    char f_comments[255+1];
    char f_reversal_flag[1+1];
    char f_cancle_flag[1+1];
    char f_clear_date[8+1];
    char f_merchant_name_add[60+1];
    long f_merchant_fee;
}FILE_ACOMA_ST;

typedef struct {
    long f_id;
    char f_file_date[8+1];
    int  f_channel_no;
    char f_agent_code[11+1];
    char f_snd_code[11+1];
    char f_rcv_code[11+1];
    char f_iss_code[11+1];
    char f_trace_no[6+1];
    char f_tran_time[10+1];
    char f_card_no[19+1];
    char f_card_type[2+1];  //
    long f_tran_amt;
    int f_tran_type;
    char f_mcc[4+1];
    char f_merchant_no[15+1];
    char f_terminal_no[8+1];
    char f_tran_rrn[12+1];
    //char f_auth_no[6+1];
    char f_msg_code[4+1];
    char f_tran_code[6+1];
    char f_sub_tran_code[2+1];
    //char f_resp_code[2+1];
    //char f_pos_mode[4+1];
    //long f_rcv_fee;
    long f_acq_fee; //
    //long f_pay_fee;
    long f_lfee; //
    long f_net_amt; //
    char f_org_info[42+1]; //
    //long f_tran_fee;
    //char f_card_sn[3+1];
    //char f_org_trace_no[6+1];
    //char f_org_tran_time[10+1];
    //char f_data_acoma[1024+1];
    char f_data_lfee[1024+1];
    char f_comments[255+1];
    //char f_reversal_flag[1+1];
    //char f_cancle_flag[1+1];
    //char f_clear_date[8+1];
    //char f_merchant_name_add[60+1];
    //long f_merchant_fee;
}FILE_LFEE_ST;

typedef struct
{
    long    system_ref                    ; //    系统流水号    【唯一索引】A_SYS
    char    order_no            [36 + 1]; //    订单号    【唯一索引】互联网：订单号补0到32位+电商ID补0到4位。【预留】通道：主机流水号补0到32位+主机ID补0到4位
    char    trace_index1        [35 + 1]; //    对下商户索引    商户终端批号流水【唯一索引】
    char    trace_index2        [58 + 1]; //
    long    org_systemref                ; //    原交易系统流水号    无则为0
    char    org_mesg            [42 + 1]; //
    char    trace_orgindex1        [35 + 1]; //    原始流水表中索引    【索引】INDEX1部分
    long    acct_trace_begin            ; //    分录账户流水号
    int     cut_date                    ; //    日切日期
    int     system_date                    ; //    系统日期
    int     system_time                    ; //    系统时间
    int     branch_map_id               ;
    long    branch_code                    ; //    分支编码    【索引】商户所属
    int     partner_map_id              ;
    long    partner_code                ; //    归属编码    【索引】无则为0
    int     trans_id                    ; //    交易类型
    int     trans_subid                    ; //    子交易类型
    char    trans_subname        [40 + 1]; //    交易类型
    char    trans_type            [1  + 1]; //    处理方式    0：系统  1：手工
    char    trans_retcode        [2  + 1]; //    返回码
    char    trans_retdesc        [40 + 1]; //    返回说明
    char    trans_status        [1  + 1]; //    流水标志    0：成功 1失败 9处理中
    char    trans_voidflag        [1  + 1]; //    交易标识    0正常 1撤销
    char    trans_reverflag        [1  + 1]; //    交易标识    0正常 1冲正
    char    trans_refundflag    [1  + 1]; //    交易标识    0正常 1退货
    char    trans_ic            [510+ 1]; //    IC明细数据
    char    shop_no                [15 + 1]; //    商户号    【索引】
    char    shop_nameab            [60 + 1]; //    商户名称    对下
    int     county_id                   ;
    char    pos_no                [8  + 1]; //    POS号    对下
    char    pos_mode            [4  + 1]; //    输入模式
    char    card_sn             [3  + 1];
    char    pos_service         [2  + 1];
    char    pos_pincap          [2  + 1];
    char    proc_code           [6  + 1];
    char    pos_batch            [6  + 1]; //    POS批次号
    char    pos_trace            [6  + 1]; //    POS流水号
    int     pos_date                    ; //    POS交易日期    【索引】
    int     pos_time                    ; //    POS交易时间
    int     host_map_id                    ; //    路由主机    【索引】+HOST_TRACE组合唯一
    char    host_acq_code        [11 + 1]; //    分配代理号    无则为全0
    char    host_trace            [12 + 1]; //    主机流水
    char    host_auth            [6  + 1]; //    授权号
    char    hshop_no            [15 + 1]; //    商户号    对上
    char    hshop_nameab        [60 + 1]; //    商户名称    对上
    char    hpos_no                [8  + 1]; //    POS号    对上
    char    hpos_batch            [6  + 1]; //    POS批次号
    char    hpos_trace            [6  + 1]; //    POS流水号
    char    card_bank_id          [20+1]; //    银行代码
    char    card_bank_name        [60 + 1]; //    银行名称
    char    card_no                [19 + 1]; //    卡号    【索引】
    char    card_exp            [4  + 1]; //    卡有效期
    char    card_flag            [1  + 1]; //    卡标志    见卡表（区分借贷记卡）
    int     pay_type                    ; //    支付方式    【索引】见A_PAYTYPE表
    long    trans_amt                    ; //    交易金额
    long    trans_refamt                ; //    交易金额
    long    shop_net_amt                ; //    交易净金额    商户方
    long    shop_fee_amt                ; //    交易总手续费    为商户手续费总和=收单+增值
    long    shop_mdr_amt                ; //    收单手续费    =通道收单成本+平台收单收益
    long    shop_avr_amt                ; //    增值手续费    预留 0 = 增值会员收益+增值平台收益
    long    mdr_host_amt                ; //    通道收单成本金额
    long    mdr_iss_amt                    ; //    通道收单成本金额
    long    mdr_cup_amt                    ; //    通道收单成本金额
    long    mdr_logo_amt                ; //    通道收单成本金额
    long    mdr_plat_amt                ; //    平台收单收益金额
    long    mdr_partner_amt             ;
    char    settle_pos_flag        [1  + 1]; //    POS结算标志    0：平衡 1:终端多 2:平台多 9异常；0
    char    settle_host_flag    [1  + 1]; //    通道对账标志    0：平衡 1：追偿 2：挂账 3：待对 9：异常
    long    settle_plat_amt                ; //    通道应收清算金额    平台
    long    settle_host_amt                ; //    通道实收清算金额    通道/渠道初始值同
    char    host_clear_type        [1  + 1]; //    通道结算方式    0：收单模式（二清） 1：代理模式
    int     host_clear_date                ; //    通道清算日期    结算日期
    char    shop_clear_type        [1  + 1]; //    商户清算方式    0：商户直收1通道直接到商户 2渠道给商户 3平台给商户 4平台给集团商户 5平台给代理
    int     shop_clear_date                ; //    商户清算日期    结算日期
    int     branch_clear_date            ; //    分支分润清算日期    结算日期
    int     partner_clear_date            ; //    分支分润清算日期    结算日期
    char    error_point            [1  + 1]; //    差错标志    0：正常 1拒付 2调单 见差错流程表
    char    risk_flag            [1  + 1]; //    风险等级    0：正常，其他见几级风险规则
    char    mac                    [32 + 1]; //    MAC押码    所有金额
    char    res                    [90 + 1]; //   备注    可保存风险信息
}A_TRACE_ST;

typedef struct {
    char system_status[1+1];
    int system_batch;
    long system_ref;
    char settle_date[8+1];
}A_SYS_ST;

typedef struct
{
    long    card_bin             ;
    char    card_name [60 + 1]   ;
    int     card_track           ;
    int     card_off             ;
    int     card_len             ;
    int     pay_type             ;
    char    bank_id   [20+1]     ;
    char    bank_name [60 + 1]   ;
    char    card_flag [1  + 1]   ;
    int     route_map_id         ;
}A_CARDBIN_ST;

typedef struct
{
    long    f_id                 ;
    char    f_tran_type[8+1]     ;
    char    f_tran_sub_type[8+1] ;
    char    f_tran_name[100+1]   ;
    char    f_mesg_code[4+1]     ;
    char    f_tran_code[6+1]     ;
    char    f_sub_tran_code[2+1] ;
    char    f_liquidate_flag[1+1];
    char    f_org_mesg_code[4+1] ;
}T_TRAN_TYPE_ST;

typedef struct
{
    int     COUNTY_ID            ;
    char    COUNT_NAME[30+1]     ;
    char    CITY_NAME[30+1]      ;
    char    PROVINCE_NAME[30+1]  ;
    int    TEL_ID                ;
    int    ZIP_ID                ;
    int    CLEAR_CODE            ;
}A_CITY_ST;

typedef struct {
    A_SYS_ST        sys_st;
    A_TRACE_ST      org_trace_st;
    A_CARDBIN_ST    cardbin_st;
    A_CITY_ST       city_st;
    FILE_ACOMA_ST   file_acoma_st;
    FILE_ACOMA_ST   org_file_acoma_st;
    FILE_LFEE_ST    file_lfee_st;
    T_TRAN_TYPE_ST  tran_type_st;
    A_TRACE_ST      trace_st;
    char            org_index_str[42+1];  /*匹配原交易的检索条件*/
    char            cur_index_str[42+1];  /*匹配原交易的检索条件*/
    char            shop_clear_mode[1+1]; /*商户清算模式 1-通道给商户 3-平台给商户*/
}DATA_UNION_ST;

/**
 *查询系统参考号
 **/
int db_get_sys_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st )
{
    int ret ;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "SYSTEM_STATUS",  data_union_st->sys_st.system_status, "系统状态值", PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->sys_st.system_status),
        "SYSTEM_BATCH" , &data_union_st->sys_st.system_batch , "系统批次号", PTS_DB_TYPE_LONG ,  sizeof(data_union_st->sys_st.system_batch ),
        "SYSTEM_REF"   , &data_union_st->sys_st.system_ref   , "检索参考号", PTS_DB_TYPE_LONG ,  sizeof(data_union_st->sys_st.system_ref   ),
        "SETTLE_DATE"  ,  data_union_st->sys_st.settle_date  , "上次日终日", PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->sys_st.settle_date  ),
        ""             , NULL                              , NULL        , 0                ,  0};
        
    strcpy (sql_info    , "SELECT A_SYS");
    sprintf (sql         , "SELECT * FROM %s.A_SYS WHERE 1 FOR UPDATE ", db_posp_name);

    ret = db_begin (conn_ptr);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (-1);
    }

    ret = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (ret);
    }

    memset (sql         , 0, sizeof(sql));
    memset (sql_info    , 0, sizeof(sql_info));
    strcpy (sql_info    , "UPDATE A_SYS");
    sprintf(sql         , "UPDATE %s.A_SYS SET SYSTEM_REF='%ld'", db_posp_name , data_union_st->sys_st.system_ref + 1);

    ret = db_update (conn_ptr, sql, sql_info);
    if (ret <= 0)
    {
        db_rollback (conn_ptr);
        return (-1);
    }

    ret = db_commit (conn_ptr);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (-1);
    }
    
    return (0);
}

int db_update_org_trace_refund (MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret ;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };

    memset (sql         , 0, sizeof(sql));
    strcpy (sql_info    , "UPDATE A_TRACE.ORG_REFUND");
    sprintf(sql         , "UPDATE %s.A_TRACE SET TRANS_REFUNDFLAG = \'1\', TRANS_REFAMT = TRANS_REFAMT - %ld WHERE ORG_MESG = \'%s\' ", db_posp_name , data_union_st->file_acoma_st.f_tran_amt, data_union_st->org_index_str);

    ret = db_update (conn_ptr, sql, sql_info);
    if (ret < 0)
    {
        return (-1);
    }
    else
    if (ret == 0)
    {
        strcpy (sql_info    , "UPDATE A_TRACE_HIS.ORG_REFUND");
        memset (sql         , 0, sizeof(sql));
        sprintf(sql         , "UPDATE %s.A_TRACE_HIS SET TRANS_REFUNDFLAG = \'1\' , TRANS_REFAMT = TRANS_REFAMT - %ld WHERE ORG_MESG = \'%s\' ", db_posp_name , data_union_st->file_acoma_st.f_tran_amt, data_union_st->org_index_str);

        ret = db_update (conn_ptr, sql, sql_info);

        return (ret);
    }

    return (0);
}

int db_update_org_trace_cancel (MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret ;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };

    memset (sql         , 0, sizeof(sql));
    strcpy (sql_info    , "UPDATE A_TRACE.ORG_CANCEL");
    sprintf(sql         , "UPDATE %s.A_TRACE SET TRANS_VOIDFLAG = \'1\' WHERE ORG_MESG = \'%s\' ", db_posp_name , data_union_st->org_index_str);

    ret = db_update (conn_ptr, sql, sql_info);
    if (ret < 0)
    {
        return (-1);
    }

    return (ret);
}

int db_update_trace_reversal (MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret ;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };

    memset (sql         , 0, sizeof(sql));
    strcpy (sql_info    , "UPDATE A_TRACE.REVESAL");
    sprintf(sql         , "UPDATE %s.A_TRACE SET TRANS_REVERFLAG = \'1\' WHERE ORG_MESG = \'%s\' ", db_posp_name , data_union_st->org_index_str);

    ret = db_update (conn_ptr, sql, sql_info);
    if (ret < 0)
    {
        return (-1);
    }

    return (ret);
}

/**
 *获取品牌服务费
 **/
int db_get_file_lfee_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "f_id"               , &data_union_st->file_lfee_st.f_id                 , "业务id                      ",   PTS_DB_TYPE_LONG ,  sizeof(data_union_st->file_lfee_st.f_id    ),
        "f_file_date"        , data_union_st->file_lfee_st.f_file_date           , "文件日期                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_file_date  ),
        "f_channel_no"       , &data_union_st->file_lfee_st.f_channel_no         , "通道编号                    " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_lfee_st.f_channel_no    ),
        "f_agent_code"       , data_union_st->file_lfee_st.f_agent_code          , "代理机构编码（32域）        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_agent_code    ),
        "f_snd_code"         , data_union_st->file_lfee_st.f_snd_code            , "发送机构（33域）            " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_snd_code    ),
        "f_rcv_code"         , data_union_st->file_lfee_st.f_rcv_code            , "接受机构（100域）           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_rcv_code     ),
        "f_iss_code"         , data_union_st->file_lfee_st.f_iss_code            , "发卡机构编码                " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->file_lfee_st.f_iss_code   ),
        "f_trace_no"         , data_union_st->file_lfee_st.f_trace_no            , "系统跟踪号（11域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_trace_no   ),
        "f_tran_time"        , data_union_st->file_lfee_st.f_tran_time           , "传输时间（7域）             " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_tran_time),
        "f_card_no"          , data_union_st->file_lfee_st.f_card_no             , "卡号（2域）                 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_card_no),
        "f_card_type"        , data_union_st->file_lfee_st.f_card_type           , "卡类型                      " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_card_type),
        "f_tran_amt"         , &data_union_st->file_lfee_st.f_tran_amt           , "交易金额                    " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_tran_amt),
        "f_tran_type"        , &data_union_st->file_lfee_st.f_tran_type          , "系统交易类型                " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_lfee_st.f_tran_type),
        "f_mcc"              , data_union_st->file_lfee_st.f_mcc                 , "mcc                         " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_mcc),
        "f_merchant_no"      , data_union_st->file_lfee_st.f_merchant_no         , "商户号（42域）              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_merchant_no),
        "f_terminal_no"      , data_union_st->file_lfee_st.f_terminal_no         , "终端号（41域）              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_terminal_no),
        "f_tran_rrn"         , data_union_st->file_lfee_st.f_tran_rrn            , "交易参考号（37域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_tran_rrn),
        //"f_auth_no"          , data_union_st->file_lfee_st.f_auth_no             , "授权应答码（38域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_auth_no),
        "f_msg_code"         , data_union_st->file_lfee_st.f_msg_code            , "报文类型                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_msg_code),
        "f_tran_code"        , data_union_st->file_lfee_st.f_tran_code           , "交易类型码（3域）           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_tran_code),
        "f_sub_tran_code"    , data_union_st->file_lfee_st.f_sub_tran_code       , "服务店条件码（25域）        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_sub_tran_code),
        //"f_resp_code"        , data_union_st->file_lfee_st.f_resp_code           , "交易返回码（39域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_resp_code),
        //"f_pos_mode"         , data_union_st->file_lfee_st.f_pos_mode            , "服务店输入方式              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_pos_mode),
        "f_acq_fee"          , &data_union_st->file_lfee_st.f_acq_fee            , "受理方应收手续费            " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_acq_fee),
        "f_lfee"             , &data_union_st->file_lfee_st.f_lfee               , "品牌服务费                  " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_lfee),
        "f_net_amt"          , &data_union_st->file_lfee_st.f_net_amt            , "交易净额                    " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_net_amt),
        "f_org_info"         , data_union_st->file_lfee_st.f_org_info            , "原交易信息                  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_org_info),
        //"f_org_trace_no"     , data_union_st->file_lfee_st.f_org_trace_no        , "原始交易系统跟踪号（90.2域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_org_trace_no),
        //"f_org_tran_time"    , data_union_st->file_lfee_st.f_org_tran_time       , "原始交易日期时间（90.3域）  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_org_tran_time),
        "f_data_lfee "       , data_union_st->file_lfee_st.f_data_lfee           , "品牌服务费文件一整行        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_data_lfee),
        "f_comments"         , data_union_st->file_lfee_st.f_comments            , "备注                        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_comments),
        //"f_reversal_flag"    , data_union_st->file_lfee_st.f_reversal_flag       , "冲正标识 R-已冲正 空格-正常 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_reversal_flag),
        //"f_cancle_flag"      , data_union_st->file_lfee_st.f_cancle_flag         , "撤销标识 C-已撤销 空格-正常 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_cancle_flag),
        //"f_clear_date"       , data_union_st->file_lfee_st.f_clear_date          , "清算日期                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_clear_date),
        //"f_merchant_name_add", data_union_st->file_lfee_st.f_merchant_name_add   , "商户名称地址                " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_merchant_name_add),
        //"f_merchant_fee"     , &data_union_st->file_lfee_st.f_merchant_fee       , "商户手续费                  " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_merchant_fee),
        ""                   , NULL                                               , NULL       ,   0                ,  0};
        
    strcpy (sql_info    , "SELECT T_LIQUIDATE_FILE_LFEE");
    sprintf (sql         , "SELECT * "
                             "FROM T_LIQUIDATE_FILE_LFEE t "
                            "WHERE t.f_trace_no = \'%s\' "
                            "  AND t.f_tran_time = \'%s\' "
                            "  AND t.f_file_date = \'%s\' "
                          , data_union_st->file_acoma_st.f_trace_no
                          , data_union_st->file_acoma_st.f_tran_time
                          , data_union_st->file_acoma_st.f_file_date
    );

    ret = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (ret);
    }
    
    data_union_st->trace_st.mdr_logo_amt      = data_union_st->file_lfee_st.f_lfee       ;
    
    return (0);
}

/**
 *获取原交易信息
 **/
int db_get_org_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]           = { 0 };
    char sql_info[512]       = { 0 };
    
    DB_GET_ST data [] = {
        "SYSTEM_REF"              ,    &data_union_st->org_trace_st.system_ref               ,  "SYSTEM_REF         "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.system_ref                ),
        "ORDER_NO"                ,     data_union_st->org_trace_st.order_no                 ,  "ORDER_NO           "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.order_no                ),
        "TRACE_INDEX1"            ,     data_union_st->org_trace_st.trace_index1             ,  "TRACE_INDEX1       "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trace_index1            ),
        "TRACE_INDEX2"            ,     data_union_st->org_trace_st.trace_index2             ,  "TRACE_INDEX2       "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trace_index2            ),
        "ORG_SYSTEMREF"           ,    &data_union_st->org_trace_st.org_systemref            ,  "ORG_SYSTEMREF      "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.org_systemref                ),
        "ORG_MESG"                ,     data_union_st->org_trace_st.org_mesg                 ,  "ORG_MESG           "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.org_mesg              ),
        "TRACE_ORGINDEX1"         ,     data_union_st->org_trace_st.trace_orgindex1          ,  "TRACE_ORGINDEX1    "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trace_orgindex1            ),
        "ACCT_TRACE_BEGIN"        ,    &data_union_st->org_trace_st.acct_trace_begin         ,  "ACCT_TRACE_BEGIN   "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.acct_trace_begin        ),
        "CUT_DATE"                ,    &data_union_st->org_trace_st.cut_date                 ,  "CUT_DATE           "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.cut_date                ),
        "SYSTEM_DATE"             ,    &data_union_st->org_trace_st.system_date              ,  "SYSTEM_DATE        "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.system_date                ),
        "SYSTEM_TIME"             ,    &data_union_st->org_trace_st.system_time              ,  "SYSTEM_TIME        "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.system_time                ),
        "BRANCH_CODE"             ,    &data_union_st->org_trace_st.branch_code              ,  "BRANCH_CODE        "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.branch_code                ),
        "PARTNER_CODE"            ,    &data_union_st->org_trace_st.partner_code             ,  "PARTNER_CODE       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.partner_code            ),
        "TRANS_ID"                ,    &data_union_st->org_trace_st.trans_id                 ,  "TRANS_ID           "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.trans_id                ),
        "TRANS_SUBID"             ,    &data_union_st->org_trace_st.trans_subid              ,  "TRANS_SUBID        "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.trans_subid                ),
        "TRANS_SUBNAME"           ,     data_union_st->org_trace_st.trans_subname            ,  "TRANS_SUBNAME      "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_subname            ),
        "TRANS_TYPE"              ,     data_union_st->org_trace_st.trans_type               ,  "TRANS_TYPE         "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_type                ),
        "TRANS_RETCODE"           ,     data_union_st->org_trace_st.trans_retcode            ,  "TRANS_RETCODE      "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_retcode            ),
        "TRANS_RETDESC"           ,     data_union_st->org_trace_st.trans_retdesc            ,  "TRANS_RETDESC      "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_retdesc            ),
        "TRANS_STATUS"            ,     data_union_st->org_trace_st.trans_status             ,  "TRANS_STATUS       "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_status            ),
        "TRANS_VOIDFLAG"          ,     data_union_st->org_trace_st.trans_voidflag           ,  "TRANS_VOIDFLAG     "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_voidflag            ),
        "TRANS_REVERFLAG"         ,     data_union_st->org_trace_st.trans_reverflag          ,  "TRANS_REVERFLAG    "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_reverflag            ),
        "TRANS_REFUNDFLAG"        ,     data_union_st->org_trace_st.trans_refundflag         ,  "TRANS_REFUNDFLAG   "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_refundflag        ),
        "TRANS_IC"                ,     data_union_st->org_trace_st.trans_ic                 ,  "TRANS_IC           "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.trans_ic                ),
        "SHOP_NO"                 ,     data_union_st->org_trace_st.shop_no                  ,  "SHOP_NO            "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.shop_no                    ),
        "SHOP_NAMEAB"             ,     data_union_st->org_trace_st.shop_nameab              ,  "SHOP_NAMEAB        "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.shop_nameab                ),
        "COUNTY_ID"               ,    &data_union_st->org_trace_st.county_id                ,  "COUNTY_ID          "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.county_id                ),
        "POS_NO"                  ,     data_union_st->org_trace_st.pos_no                   ,  "POS_NO             "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.pos_no                    ),
        "POS_MODE"                ,     data_union_st->org_trace_st.pos_mode                 ,  "POS_MODE           "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.pos_mode                ),
        "CARD_SN"                 ,     data_union_st->org_trace_st.card_sn                  ,  "CARD_SN            "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.card_sn                    ),
        "POS_SERVICE"             ,     data_union_st->org_trace_st.pos_service              ,  "POS_SERVICE        "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.pos_service                ),
        "POS_PINCAP"              ,     data_union_st->org_trace_st.pos_pincap               ,  "POS_PINCAP         "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.pos_pincap                ),
        "PROC_CODE"               ,     data_union_st->org_trace_st.proc_code                ,  "PROC_CODE          "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.proc_code                ),
        "POS_BATCH"               ,     data_union_st->org_trace_st.pos_batch                ,  "POS_BATCH          "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.pos_batch                ),
        "POS_TRACE"               ,     data_union_st->org_trace_st.pos_trace                ,  "POS_TRACE          "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.pos_trace                ),
        "POS_DATE"                ,    &data_union_st->org_trace_st.pos_date                 ,  "POS_DATE           "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.pos_date                ),
        "POS_TIME"                ,    &data_union_st->org_trace_st.pos_time                 ,  "POS_TIME           "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.pos_time                ),
        "HOST_MAP_ID"             ,    &data_union_st->org_trace_st.host_map_id              ,  "HOST_MAP_ID        "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.host_map_id                ),
        "HOST_ACQ_CODE"           ,     data_union_st->org_trace_st.host_acq_code            ,  "HOST_ACQ_CODE      "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.host_acq_code            ),
        "HOST_TRACE"              ,     data_union_st->org_trace_st.host_trace               ,  "HOST_TRACE         "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.host_trace                ),
        "HOST_AUTH"               ,     data_union_st->org_trace_st.host_auth                ,  "HOST_AUTH          "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.host_auth                ),
        "HSHOP_NO"                ,     data_union_st->org_trace_st.hshop_no                 ,  "HSHOP_NO           "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.hshop_no                ),
        "HSHOP_NAMEAB"            ,     data_union_st->org_trace_st.hshop_nameab             ,  "HSHOP_NAMEAB       "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.hshop_nameab            ),
        "HPOS_NO"                 ,     data_union_st->org_trace_st.hpos_no                  ,  "HPOS_NO            "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.hpos_no                    ),
        "HPOS_BATCH"              ,     data_union_st->org_trace_st.hpos_batch               ,  "HPOS_BATCH         "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.hpos_batch                ),
        "HPOS_TRACE"              ,     data_union_st->org_trace_st.hpos_trace               ,  "HPOS_TRACE         "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.hpos_trace                ),
        "CARD_BANK_ID"            ,     data_union_st->org_trace_st.card_bank_id             ,  "CARD_BANK_ID       "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.card_bank_id            ),
        "CARD_BANK_NAME"          ,     data_union_st->org_trace_st.card_bank_name           ,  "CARD_BANK_NAME     "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.card_bank_name            ),
        "CARD_NO"                 ,     data_union_st->org_trace_st.card_no                  ,  "CARD_NO            "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.card_no                    ),
        "CARD_EXP"                ,     data_union_st->org_trace_st.card_exp                 ,  "CARD_EXP           "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.card_exp                ),
        "CARD_FLAG"               ,     data_union_st->org_trace_st.card_flag                ,  "CARD_FLAG          "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.card_flag                ),
        "PAY_TYPE"                ,    &data_union_st->org_trace_st.pay_type                 ,  "PAY_TYPE           "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.pay_type                ),
        "TRANS_AMT"               ,    &data_union_st->org_trace_st.trans_amt                ,  "TRANS_AMT          "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.trans_amt                ),
        "TRANS_REFAMT"            ,    &data_union_st->org_trace_st.trans_refamt             ,  "TRANS_REFAMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.trans_refamt                ),
        "SHOP_NET_AMT"            ,    &data_union_st->org_trace_st.shop_net_amt             ,  "SHOP_NET_AMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.shop_net_amt            ),
        "SHOP_FEE_AMT"            ,    &data_union_st->org_trace_st.shop_fee_amt             ,  "SHOP_FEE_AMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.shop_fee_amt            ),
        "SHOP_MDR_AMT"            ,    &data_union_st->org_trace_st.shop_mdr_amt             ,  "SHOP_MDR_AMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.shop_mdr_amt            ),
        "SHOP_AVR_AMT"            ,    &data_union_st->org_trace_st.shop_avr_amt             ,  "SHOP_AVR_AMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.shop_avr_amt            ),
        "MDR_HOST_AMT"            ,    &data_union_st->org_trace_st.mdr_host_amt             ,  "MDR_HOST_AMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.mdr_host_amt            ),
        "MDR_ISS_AMT"             ,    &data_union_st->org_trace_st.mdr_iss_amt              ,  "MDR_ISS_AMT        "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.mdr_iss_amt            ),
        "MDR_CUP_AMT"             ,    &data_union_st->org_trace_st.mdr_cup_amt              ,  "MDR_CUP_AMT        "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.mdr_cup_amt            ),
        "MDR_LOGO_AMT"            ,    &data_union_st->org_trace_st.mdr_logo_amt             ,  "MDR_LOGO_AMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.mdr_logo_amt            ),
        "MDR_PLAT_AMT"            ,    &data_union_st->org_trace_st.mdr_plat_amt             ,  "MDR_PLAT_AMT       "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.mdr_plat_amt            ),
        "MDR_PARTNER_AMT"         ,    &data_union_st->org_trace_st.mdr_partner_amt          ,  "MDR_PARTNER_AMT    "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.mdr_partner_amt         ),
        "SETTLE_POS_FLAG"         ,     data_union_st->org_trace_st.settle_pos_flag          ,  "SETTLE_POS_FLAG    "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.settle_pos_flag            ),
        "SETTLE_HOST_FLAG"        ,     data_union_st->org_trace_st.settle_host_flag         ,  "SETTLE_HOST_FLAG   "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.settle_host_flag        ),
        "SETTLE_PLAT_AMT"         ,    &data_union_st->org_trace_st.settle_plat_amt          ,  "SETTLE_PLAT_AMT    "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.settle_plat_amt            ),
        "SETTLE_HOST_AMT"         ,    &data_union_st->org_trace_st.settle_host_amt          ,  "SETTLE_HOST_AMT    "  , PTS_DB_TYPE_LONG, sizeof(data_union_st->org_trace_st.settle_host_amt            ),
        "HOST_CLEAR_TYPE"         ,     data_union_st->org_trace_st.host_clear_type          ,  "HOST_CLEAR_TYPE    "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.host_clear_type            ),
        "HOST_CLEAR_DATE"         ,    &data_union_st->org_trace_st.host_clear_date          ,  "HOST_CLEAR_DATE    "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.host_clear_date            ),
        "SHOP_CLEAR_TYPE"         ,     data_union_st->org_trace_st.shop_clear_type          ,  "SHOP_CLEAR_TYPE    "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.shop_clear_type            ),
        "SHOP_CLEAR_DATE"         ,    &data_union_st->org_trace_st.shop_clear_date          ,  "SHOP_CLEAR_DATE    "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.shop_clear_date            ),
        "BRANCH_CLEAR_DATE"       ,    &data_union_st->org_trace_st.branch_clear_date        ,  "BRANCH_CLEAR_DATE  "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.branch_clear_date        ),
        "PARTNER_CLEAR_DATE"      ,    &data_union_st->org_trace_st.partner_clear_date       ,  "PARTNER_CLEAR_DATE "  , PTS_DB_TYPE_INT , sizeof(data_union_st->org_trace_st.partner_clear_date      ),
        "ERROR_POINT"             ,     data_union_st->org_trace_st.error_point              ,  "ERROR_POINT        "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.error_point                ),
        "RISK_FLAG"               ,     data_union_st->org_trace_st.risk_flag                ,  "RISK_FLAG          "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.risk_flag                ),
        "MAC"                     ,     data_union_st->org_trace_st.mac                      ,  "MAC                "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.mac                        ),
        "RES"                     ,     data_union_st->org_trace_st.res                      ,  "RES                "  , PTS_DB_TYPE_CHAR, sizeof(data_union_st->org_trace_st.res                        ),
        ""                        ,     NULL                                   ,  NULL                   , 0               , 0 };
        
    strcpy (sql_info   , "SELECT A_TRACE.ORG_CUR");
    sprintf(sql, "SELECT * FROM %s.A_TRACE WHERE ORG_MESG = \'%s\' ", db_posp_name , data_union_st->org_index_str );

    ret = db_begin (conn_ptr);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (-1);
    }
    
    ret = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        //当前流水查不到，差历史流水
        memset (sql_info    , 0, sizeof(sql_info));
        memset (sql    , 0, sizeof(sql));
        strcpy (sql_info    , "SELECT A_TRACE_HIS.ORG_CUR");
        sprintf(sql, "SELECT * FROM %s.A_TRACE_HIS WHERE ORG_MESG = \'%s\' ", db_posp_name , data_union_st->org_index_str );
        
        ret = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            return (ret);
        }
    }

    memset (sql         , 0, sizeof(sql));
    memset (sql_info    , 0, sizeof(sql_info));
    strcpy (sql_info    , "UPDATE A_TRACE.ORG_CUR");
    if ( atoi(data_union_st->tran_type_st.f_tran_type) == APP_CANCEL )
    {
        ret = db_update_org_trace_cancel(conn_ptr, data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            return (-1);
        }
    }
    else if ( atoi(data_union_st->tran_type_st.f_tran_type) == APP_AUTOVOID )
    {
        ret = db_update_trace_reversal(conn_ptr, data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            return (-1);
        }
    }
    else if ( data_union_st->trace_st.trans_id == APP_REFUND )
    {
        ret = db_update_org_trace_refund(conn_ptr, data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            return (-1);
        }
    }
    else
    {
        db_rollback (conn_ptr);
        return NOERR;
    }

    ret = db_commit (conn_ptr);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (-1);
    }
    
    return NOERR;
}

/**
 *获取卡bin信息
 **/
int db_get_cardbin_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "CARD_BIN"       , &data_union_st->cardbin_st.card_bin       , "卡BIN 号",   PTS_DB_TYPE_LONG ,  sizeof(data_union_st->cardbin_st.card_bin    ),
        "CARD_NAME"      ,  data_union_st->cardbin_st.card_name      , "卡种名称" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->cardbin_st.card_name   ),
        "CARD_TRACK"     , &data_union_st->cardbin_st.card_track     , "所在磁道" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_track  ),
        "CARD_OFF"       , &data_union_st->cardbin_st.card_off       , "偏移位置" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_off    ),
        "CARD_LEN"       , &data_union_st->cardbin_st.card_len       , "卡号长度" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_len    ),
        "PAY_TYPE"       , &data_union_st->cardbin_st.pay_type       , "支付类型" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.pay_type    ),
        "BANK_ID"        ,  data_union_st->cardbin_st.bank_id        , "银行代码" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->cardbin_st.bank_id     ),
        "BANK_NAME"      ,  data_union_st->cardbin_st.bank_name      , "银行名称" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->cardbin_st.bank_name   ),
        "CARD_FLAG"      , &data_union_st->cardbin_st.card_flag      , "卡类标志" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_flag   ),
        "ROUTE_MAP_ID"   , &data_union_st->cardbin_st.route_map_id   , "默认路由" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.route_map_id),
        ""               , NULL                                 , NULL       ,   0                ,  0};
    
    DelSpace(data_union_st->file_acoma_st.f_card_no);
    
    strcpy (sql_info    , "SELECT A_BIN");
    sprintf (sql         , "SELECT * "
                             "FROM %s.A_BIN t "
                            "WHERE substr(\'%s\' FROM 1 FOR length(t.card_bin)) = t.card_bin "
                            "  AND t.card_len = %d "
                          , db_posp_name
                          , data_union_st->file_acoma_st.f_card_no
                          , strlen(data_union_st->file_acoma_st.f_card_no));

    ret = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (ret);
    }

    return (0);
}

/**
 *获取交易类型
 **/
int db_get_trantype_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "f_id"                , &data_union_st->tran_type_st.f_id             , "id                      " ,   PTS_DB_TYPE_INT ,  sizeof(data_union_st->tran_type_st.f_id    ),
        "f_tran_type"         ,  data_union_st->tran_type_st.f_tran_type      , "系统交易类型            " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->tran_type_st.f_tran_type   ),
        "f_tran_sub_type"     , data_union_st->tran_type_st.f_tran_sub_type   , "系统交易子类型码        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_tran_sub_type  ),
        "f_tran_name"         , data_union_st->tran_type_st.f_tran_name       , "系统交易类型            " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_tran_name    ),
        "f_mesg_code"         , data_union_st->tran_type_st.f_mesg_code       , "消息类型码              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_mesg_code    ),
        "f_tran_code"         , data_union_st->tran_type_st.f_tran_code       , "交易类型码（3域）       " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_tran_code    ),
        "f_sub_tran_code"     , data_union_st->tran_type_st.f_sub_tran_code   , "服务店条件码（25域）    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_sub_tran_code     ),
        "f_liquidate_flag"    ,  data_union_st->tran_type_st.f_liquidate_flag , "借贷标识 D-借记 C-贷记  " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->tran_type_st.f_liquidate_flag   ),
        "f_org_mesg_code"     ,  data_union_st->tran_type_st.f_org_mesg_code  , "原始报文类型            " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->tran_type_st.f_org_mesg_code   ),
        ""                    , NULL                                          , NULL                       ,   0                ,  0};
        
    strcpy (sql_info    , "SELECT T_TRAN_TYPE");
    sprintf (sql         , "SELECT * "
                             "FROM T_TRAN_TYPE t "
                            "WHERE t.f_mesg_code = \'%s\' "
                            "  AND t.f_tran_code = \'%s\' "
                            "  AND t.f_sub_tran_code = \'%s\' "
                          , data_union_st->file_acoma_st.f_msg_code
                          , data_union_st->file_acoma_st.f_tran_code
                          , data_union_st->file_acoma_st.f_sub_tran_code
    );

    ret = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (ret);
    }
    
    //银联中心为 32 33 7 11组合唯一；匹配原交易
    sprintf( data_union_st->org_index_str, "%s%s%s000%s000%s"
            , data_union_st->tran_type_st.f_org_mesg_code
            , data_union_st->file_acoma_st.f_org_trace_no
            , data_union_st->file_acoma_st.f_org_tran_time
            , data_union_st->file_acoma_st.f_agent_code
            , data_union_st->file_acoma_st.f_snd_code
    );
    
    /*付款类，变换交易金额正负号*/
    if ( data_union_st->tran_type_st.f_liquidate_flag[0] == DC_FLAG_C )
    {
        data_union_st->file_acoma_st.f_tran_amt = -1 * data_union_st->file_acoma_st.f_tran_amt ;
    }
    
    return (0);
}

/**
 *获取交易地区信息
 **/
int db_get_city_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "COUNTY_ID"       , &data_union_st->city_st.COUNTY_ID       , "自增值   ",   PTS_DB_TYPE_INT ,  sizeof(data_union_st->city_st.COUNTY_ID    ),
        "COUNT_NAME"      ,  data_union_st->city_st.COUNT_NAME      , "区县名称" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->city_st.COUNT_NAME   ),
        "CITY_NAME"       , data_union_st->city_st.CITY_NAME        , "地市名称" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->city_st.CITY_NAME  ),
        "PROVINCE_NAME"   , data_union_st->city_st.PROVINCE_NAME    , "省份名称" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->city_st.PROVINCE_NAME    ),
        "TEL_ID"          , &data_union_st->city_st.TEL_ID          , "电话区号" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->city_st.TEL_ID    ),
        "ZIP_ID"          , &data_union_st->city_st.ZIP_ID          , "邮政编码" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->city_st.ZIP_ID    ),
        "CLEAR_CODE"      , &data_union_st->city_st.CLEAR_CODE      , "区域代码" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->city_st.CLEAR_CODE     ),
        ""                , NULL                                 , NULL       ,   0                ,  0};
        
    strcpy (sql_info    , "SELECT A_CITY");
    sprintf (sql         , "SELECT * "
                             "FROM %s.A_CITY t "
                            "WHERE t.clear_code = %d "
                          , db_posp_name
                          , atoi(data_union_st->file_acoma_st.f_agent_code + 4)
    );

    ret = db_select_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (ret);
    }

    return (0);
}


/**
 *取待登记流水的数据
 **/
int db_get_file_acoma_info(DB_RESULT * db_result, DATA_UNION_ST *data_union_st, char *sql_info)
{
    int ret;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "f_id"               , &data_union_st->file_acoma_st.f_id                 , "业务id                      ",   PTS_DB_TYPE_LONG ,  sizeof(data_union_st->file_acoma_st.f_id    ),
        "f_file_date"        , data_union_st->file_acoma_st.f_file_date           , "文件日期                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_file_date  ),
        "f_channel_no"       , &data_union_st->file_acoma_st.f_channel_no         , "通道编号                    " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_acoma_st.f_channel_no    ),
        "f_agent_code"       , data_union_st->file_acoma_st.f_agent_code          , "代理机构编码（32域）        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_agent_code    ),
        "f_snd_code"         , data_union_st->file_acoma_st.f_snd_code            , "发送机构（33域）            " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_snd_code    ),
        "f_rcv_code"         , data_union_st->file_acoma_st.f_rcv_code            , "接受机构（100域）           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_rcv_code     ),
        "f_iss_code"         , data_union_st->file_acoma_st.f_iss_code            , "发卡机构编码                " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->file_acoma_st.f_iss_code   ),
        "f_trace_no"         , data_union_st->file_acoma_st.f_trace_no            , "系统跟踪号（11域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_trace_no   ),
        "f_tran_time"        , data_union_st->file_acoma_st.f_tran_time           , "传输时间（7域）             " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_tran_time),
        "f_card_no"          , data_union_st->file_acoma_st.f_card_no             , "卡号（2域）                 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_card_no),
        "f_tran_amt"         , &data_union_st->file_acoma_st.f_tran_amt           , "交易金额                    " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_tran_amt),
        "f_tran_type"        , &data_union_st->file_acoma_st.f_tran_type          , "系统交易类型                " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_acoma_st.f_tran_type),
        "f_mcc"              , data_union_st->file_acoma_st.f_mcc                 , "mcc                         " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_mcc),
        "f_merchant_no"      , data_union_st->file_acoma_st.f_merchant_no         , "商户号（42域）              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_merchant_no),
        "f_terminal_no"      , data_union_st->file_acoma_st.f_terminal_no         , "终端号（41域）              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_terminal_no),
        "f_tran_rrn"         , data_union_st->file_acoma_st.f_tran_rrn            , "交易参考号（37域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_tran_rrn),
        "f_auth_no"          , data_union_st->file_acoma_st.f_auth_no             , "授权应答码（38域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_auth_no),
        "f_msg_code"         , data_union_st->file_acoma_st.f_msg_code            , "报文类型                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_msg_code),
        "f_tran_code"        , data_union_st->file_acoma_st.f_tran_code           , "交易类型码（3域）           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_tran_code),
        "f_sub_tran_code"    , data_union_st->file_acoma_st.f_sub_tran_code       , "服务店条件码（25域）        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_sub_tran_code),
        "f_resp_code"        , data_union_st->file_acoma_st.f_resp_code           , "交易返回码（39域）          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_resp_code),
        "f_pos_mode"         , data_union_st->file_acoma_st.f_pos_mode            , "服务店输入方式              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_pos_mode),
        "f_rcv_fee"          , &data_union_st->file_acoma_st.f_rcv_fee            , "受理方应收手续费            " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_rcv_fee),
        "f_pay_fee"          , &data_union_st->file_acoma_st.f_pay_fee            , "受理方应付手续费            " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_pay_fee),
        "f_tran_fee"         , &data_union_st->file_acoma_st.f_tran_fee           , "转接服务费（X+n11）         " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_tran_fee),
        "f_card_sn"          , data_union_st->file_acoma_st.f_card_sn             , "卡片序列号                  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_card_sn),
        "f_org_trace_no"     , data_union_st->file_acoma_st.f_org_trace_no        , "原始交易系统跟踪号（90.2域）" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_org_trace_no),
        "f_org_tran_time"    , data_union_st->file_acoma_st.f_org_tran_time       , "原始交易日期时间（90.3域）  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_org_tran_time),
        "f_data_acoma"       , data_union_st->file_acoma_st.f_data_acoma          , "清算文件一整行              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_data_acoma),
        "f_comments"         , data_union_st->file_acoma_st.f_comments            , "备注                        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_comments),
        "f_reversal_flag"    , data_union_st->file_acoma_st.f_reversal_flag       , "冲正标识 R-已冲正 空格-正常 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_reversal_flag),
        "f_cancle_flag"      , data_union_st->file_acoma_st.f_cancle_flag         , "撤销标识 C-已撤销 空格-正常 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_cancle_flag),
        "f_clear_date"       , data_union_st->file_acoma_st.f_clear_date          , "清算日期                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_clear_date),
        "f_merchant_name_add", data_union_st->file_acoma_st.f_merchant_name_add   , "商户名称地址                " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_merchant_name_add),
        "f_merchant_fee"     , &data_union_st->file_acoma_st.f_merchant_fee       , "商户手续费                  " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_merchant_fee),
        ""                   , NULL                                               , NULL       ,   0                ,  0};

    ret = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        return (ret);
    }
    
    //银联中心为 32 33 7 11组合唯一；匹配本交易，防止重复插入
    sprintf( data_union_st->cur_index_str, "%s%s%s000%s000%s"
            , data_union_st->file_acoma_st.f_msg_code
            , data_union_st->file_acoma_st.f_trace_no
            , data_union_st->file_acoma_st.f_tran_time
            , data_union_st->file_acoma_st.f_agent_code
            , data_union_st->file_acoma_st.f_snd_code
    );
    
    return (0);
}

/*准备流水数据*/
int func_prepare_trace_st(DATA_UNION_ST *data_union_st)
{
    //0. 随机生成pos上送交易信息（批次号的生成需要改造）
    //30. POS批次号
    {
        sprintf( data_union_st->trace_st.pos_trace, "%06ld", data_union_st->sys_st.system_ref % 1000000 );  //系统跟踪号
    }
    
    //31. POS流水号
    {
        sprintf( data_union_st->trace_st.pos_batch, "%06ld", data_union_st->sys_st.system_ref / 1000000 );  //系统跟踪号
    }
    
    //32. POS交易日期
    {
        /*日切引起的日期差异*/
        if ( strncmp( data_union_st->file_acoma_st.f_file_date + 4, data_union_st->file_acoma_st.f_tran_time, 4 ) )
        {
            data_union_st->trace_st.pos_date = _FunDateAdd( data_union_st->file_acoma_st.f_file_date , -1);
        }
        else
        {
            data_union_st->trace_st.pos_date = atoi( data_union_st->file_acoma_st.f_file_date);
        }
    }

    //33. POS交易时间
    {
        data_union_st->trace_st.pos_time = atoi( data_union_st->file_acoma_st.f_tran_time+4 );
    }
        
    {
        strcpy( data_union_st->trace_st.shop_no, data_union_st->file_acoma_st.f_merchant_no );
        strcpy( data_union_st->trace_st.pos_no, data_union_st->file_acoma_st.f_terminal_no );
        strcpy( data_union_st->trace_st.trans_retcode, data_union_st->file_acoma_st.f_resp_code );
        strcpy( data_union_st->trace_st.shop_nameab, data_union_st->file_acoma_st.f_merchant_name_add );
    }

    //7. 8 系统日期/系统时间
    {
        data_union_st->trace_st.system_date = atoi(data_union_st->file_acoma_st.f_file_date);
        data_union_st->trace_st.system_time = atoi(data_union_st->file_acoma_st.f_tran_time + 4);

        data_union_st->trace_st.cut_date    = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //1. 系统流水号    【唯一索引】
    {
        data_union_st->trace_st.system_ref = data_union_st->sys_st.system_ref; //
    }

    //2. 订单号 【返回需要UPT】
    {
        sprintf(data_union_st->trace_st.order_no     , "%032ld" , data_union_st->sys_st.system_ref); //
        sprintf(data_union_st->trace_st.order_no + 32, "%04d"   , 1); //通道编号
    }

    //3. 内部商户索引    商户终端批号流水【唯一索引】
    {
        memcpy (data_union_st->trace_st.trace_index1+0      , data_union_st->trace_st.shop_no                  , 15);
        memcpy (data_union_st->trace_st.trace_index1+15      , data_union_st->trace_st.pos_no                   ,  8);
        memcpy (data_union_st->trace_st.trace_index1+23      , data_union_st->trace_st.pos_batch         ,  6);
        memcpy (data_union_st->trace_st.trace_index1+29      , data_union_st->trace_st.pos_trace                ,  6);
    }

    //4. 商户索引（预授权与退货类专用）    商户终端批号流水【唯一索引】
    {
        if ( atoi(data_union_st->tran_type_st.f_tran_type) == APP_PURCHASE )
        {
            memcpy (data_union_st->trace_st.trace_index2+0  , data_union_st->file_acoma_st.f_msg_code         , 4);
    		    memcpy (data_union_st->trace_st.trace_index2+4  , data_union_st->file_acoma_st.f_file_date          ,  8);
    		    memcpy (data_union_st->trace_st.trace_index2+12 , data_union_st->trace_st.shop_no  		 , 15);
    		    memcpy (data_union_st->trace_st.trace_index2+27 , "000000000000000000000"             , 19);
    		    memcpy (data_union_st->trace_st.trace_index2+27 , data_union_st->file_acoma_st.f_card_no           , strlen(data_union_st->file_acoma_st.f_card_no));
    		    sprintf (data_union_st->trace_st.trace_index2+46 , "%012ld", data_union_st->trace_st.system_ref );
        }
        else
        {
            //初始唯一就可以，响应的时候修改。
            memcpy (data_union_st->trace_st.trace_index2+0      , data_union_st->file_acoma_st.f_msg_code             ,  4);
            sprintf (data_union_st->trace_st.trace_index2+4      , "%08ld", data_union_st->trace_st.pos_date);
            memcpy (data_union_st->trace_st.trace_index2+12      , data_union_st->trace_st.shop_no               , 15);
            sprintf (data_union_st->trace_st.trace_index2+27    , "%012ld", data_union_st->trace_st.system_ref);
        }
    }

    //4. 原交易系统流水号
    {
        data_union_st->trace_st.org_systemref = data_union_st->org_trace_st.system_ref;
    }

    //4.银联专用 商户终端批号流水
    {
        //银联中心为 32 33 7 11组合唯一；所以考虑退货原始数据情况
        memcpy (data_union_st->trace_st.org_mesg+0      , data_union_st->file_acoma_st.f_msg_code             ,  4);
        memcpy (data_union_st->trace_st.org_mesg+4      , data_union_st->file_acoma_st.f_trace_no             ,  6);
        memcpy (data_union_st->trace_st.org_mesg+10      , data_union_st->file_acoma_st.f_tran_time        , 10);
        memcpy (data_union_st->trace_st.org_mesg+20      , "000"                                   ,  3);
        memcpy (data_union_st->trace_st.org_mesg+23      , data_union_st->file_acoma_st.f_agent_code          ,  8);
        memcpy (data_union_st->trace_st.org_mesg+31      , "000"                                   ,  3);
        memcpy (data_union_st->trace_st.org_mesg+34      , data_union_st->file_acoma_st.f_snd_code          ,  8);
    }

    //5. 原始流水表中索引
    {
        if (atoi(data_union_st->tran_type_st.f_tran_type) != APP_CANCEL && atoi(data_union_st->tran_type_st.f_tran_type) != APP_REFUND)
        {
            memcpy (data_union_st->trace_st.trace_orgindex1    , data_union_st->trace_st.trace_index1      , 35);
        }
        else
        {
            memcpy (data_union_st->trace_st.trace_orgindex1    , data_union_st->org_trace_st.trace_index1        , 35);
        }
    }

    //6. 分录账户流水号
    {
        data_union_st->trace_st.acct_trace_begin = 0;
    }
    
    //11. 12 分支编号
    {
        data_union_st->trace_st.branch_map_id   = 0;
        data_union_st->trace_st.branch_code     = 0;
    }

    //13. 14 归属合作伙伴
    {
        data_union_st->trace_st.partner_map_id  = 0;
        data_union_st->trace_st.partner_code    = 0;
    }

    //15. 16. 17. 18 交易类型  处理方式
    {
        data_union_st->trace_st.trans_id        = atoi(data_union_st->tran_type_st.f_tran_type);
        data_union_st->trace_st.trans_subid     = atoi(data_union_st->tran_type_st.f_tran_sub_type);
        data_union_st->trace_st.trans_type[0]   = '0';
        memcpy (data_union_st->trace_st.trans_subname, data_union_st->tran_type_st.f_tran_name   , 40);
    }

    //19. 20. 交易结果
    {
        if (memcmp (data_union_st->trace_st.trans_retcode, "00", 2) == 0)
            memcpy (data_union_st->trace_st.trans_retdesc    ,  "交易成功" , 10);
        else
            memcpy (data_union_st->trace_st.trans_retdesc    ,  "交易失败" , 10);
    }

    //21 流水标志 0：成功 1失败 9处理中
    {
        if (memcmp (data_union_st->trace_st.trans_retcode, "00" , 2) == 0)
        {
            data_union_st->trace_st.trans_status[0] = '0';
        }
        else
        {
            data_union_st->trace_st.trans_status[0] = '1';
        }
    }

    //22.23.24 交易标识    0正常 1撤销 1冲正 1退货
    {
        data_union_st->trace_st.trans_voidflag    [0] = '0';
        data_union_st->trace_st.trans_reverflag    [0] = '0';
        data_union_st->trace_st.trans_refundflag    [0] = '0';
    }

    //25. IC明细数据
    {
        strcpy( data_union_st->trace_st.trans_ic, " " );   //    IC明细数据
    }

    //26. 27  商户号
    {
        strcpy (data_union_st->trace_st.shop_nameab , data_union_st->trace_st.shop_nameab );
    }
    
    {
        data_union_st->trace_st.county_id = data_union_st->city_st.COUNTY_ID;
    }
    

    // 28 POS 号
    {
    }

    //29. 输入模式
    {
        memcpy (data_union_st->trace_st.pos_mode,data_union_st->file_acoma_st.f_pos_mode, 4);
    }

    strcpy (data_union_st->trace_st.card_sn    , data_union_st->file_acoma_st.f_card_sn );
    strcpy (data_union_st->trace_st.pos_service, data_union_st->file_acoma_st.f_sub_tran_code );
    strcpy (data_union_st->trace_st.pos_pincap , "-");
    strcpy (data_union_st->trace_st.proc_code  , data_union_st->file_acoma_st.f_tran_code);
    
    //34. 路由主机ID 和 商户结算方式 和
    {
        data_union_st->trace_st.host_map_id    = CHANNEL_CUP_DIRECT    ;  //0-民生 1-银联间连 2-银联直连 3-银联代付
    }

    //35. 分配代理号
    {
        strcpy (data_union_st->trace_st.host_acq_code, data_union_st->file_acoma_st.f_agent_code );
    }

    //36. 主机流水
    {
        sprintf (data_union_st->trace_st.host_trace, "%012ld", data_union_st->sys_st.system_ref);
    }

    //37. 授权号
    {
        memcpy (data_union_st->trace_st.host_auth, data_union_st->file_acoma_st.f_auth_no, 6);
    }

    //38. 商户号(对上)
    {
        memcpy (data_union_st->trace_st.hshop_no, data_union_st->trace_st.shop_no, 15);
    }

    //39. 商户名称(对上)
    {
        memcpy (data_union_st->trace_st.hshop_nameab, data_union_st->trace_st.shop_nameab, 60);
    }

    //40. POS号对上 /40 POS批次号 /41 POS流水号
    {
        memcpy (data_union_st->trace_st.hpos_no   , data_union_st->trace_st.pos_no, 8);
        strcpy( data_union_st->trace_st.hpos_batch, data_union_st->trace_st.pos_batch );
        strcpy( data_union_st->trace_st.hpos_trace, data_union_st->file_acoma_st.f_trace_no );
    }

    //42. 43. 银行、44 卡号/  45有效期 /46卡类型
    {
        
        memcpy (data_union_st->trace_st.card_no         ,  data_union_st->file_acoma_st.f_card_no     , 19);
        strcpy (data_union_st->trace_st.card_exp         ,  "-");
        memcpy (data_union_st->trace_st.card_bank_name,  data_union_st->cardbin_st.bank_name, 60);
        strcpy (data_union_st->trace_st.card_bank_id , data_union_st->cardbin_st.bank_id);
        data_union_st->trace_st.card_flag[0]  = data_union_st->cardbin_st.card_flag[0];
    }

    //54. 支付方式
    {
        data_union_st->trace_st.pay_type = data_union_st->cardbin_st.pay_type;
    }

    //55. 交易金额
    {
        data_union_st->trace_st.trans_amt       =  data_union_st->file_acoma_st.f_tran_amt;
        data_union_st->trace_st.trans_refamt       =  data_union_st->file_acoma_st.f_tran_amt;
    }
    
    //57 58 59 60 61 62 63 65 66 67 68 69 70 71 手续费
    {
        data_union_st->trace_st.shop_net_amt	     =  data_union_st->trace_st.trans_amt - data_union_st->file_acoma_st.f_merchant_fee; //	交易净金额	商户方（商户结算金额）
        data_union_st->trace_st.shop_fee_amt	     =  data_union_st->file_acoma_st.f_merchant_fee; //	交易总手续费	为商户手续费总和=收单+增值
        data_union_st->trace_st.shop_mdr_amt	     =  data_union_st->file_acoma_st.f_merchant_fee; //	收单手续费	=通道收单成本+平台收单收益
        data_union_st->trace_st.shop_avr_amt	     =  0; //	增值手续费	预留 0 = 增值会员收益+增值平台收益

        data_union_st->trace_st.mdr_host_amt      =  data_union_st->file_acoma_st.f_merchant_fee - (data_union_st->file_acoma_st.f_rcv_fee - data_union_st->file_acoma_st.f_pay_fee)     ; //通道成本（商户手续费-收单收益）
        data_union_st->trace_st.mdr_iss_amt       =  data_union_st->file_acoma_st.f_merchant_fee - (data_union_st->file_acoma_st.f_rcv_fee - data_union_st->file_acoma_st.f_pay_fee) - data_union_st->file_acoma_st.f_tran_fee     ; //发卡费=商户手续费-收单收益 - 转接
        data_union_st->trace_st.mdr_cup_amt       =  data_union_st->file_acoma_st.f_tran_fee       ;
        //data_union_st->trace_st.mdr_logo_amt      =  0      ;
        data_union_st->trace_st.mdr_partner_amt   =  0   ;
        data_union_st->trace_st.mdr_plat_amt	    =  (data_union_st->file_acoma_st.f_rcv_fee - data_union_st->file_acoma_st.f_pay_fee)     ;
        //机构收到的清算款
        if ( atoi(data_union_st->shop_clear_mode) == SHOP_STL_MODE_ACQ ) //自主清算
        {
            data_union_st->trace_st.settle_plat_amt = data_union_st->trace_st.trans_amt - data_union_st->trace_st.mdr_host_amt ;
        }
        else if ( atoi(data_union_st->shop_clear_mode) == SHOP_STL_MODE_FUND ) //代理清算
        {
            data_union_st->trace_st.settle_plat_amt = data_union_st->trace_st.mdr_plat_amt;
        }
    }
    
    //72. POS结算勾对标志（保留）     0：平衡 1:终端多 2:平台多 9异常
    {
        //  if (app_data->system_st.dup_flag == 1)
        //      data_union_st->trace_st.settle_pos_flag [0] = '1';
        //  else
            data_union_st->trace_st.settle_pos_flag [0] = '0';
    }

    //73. 通道对账    0：平衡 1：追偿 2：挂账 3：待对 9：异常
    {
        data_union_st->trace_st.settle_host_flag[0] = '0';
    }

    //74 75 通道应收清算金额
    {
        // settle_plat_amtsettle_plat_amt ; //    通道应收清算金额    平台
        data_union_st->trace_st.settle_host_amt = data_union_st->trace_st.settle_plat_amt; //    通道实收清算金额    通道/渠道初始值同
    }

    //76 77 通道结算方式    0：收单模式（二清） 1：代理模式
    {
        data_union_st->trace_st.host_clear_type[0] = '1';

        //data_union_st->trace_st.host_clear_date    = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.host_clear_date    = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //78 79 商户清算方式    0：商户直收1通道直接到商户 2渠道给商户 3平台给商户 4平台给集团商户 5平台给代理
    {
        data_union_st->trace_st.shop_clear_type[0] = data_union_st->shop_clear_mode[0];

        //data_union_st->trace_st.shop_clear_date      = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.shop_clear_date      = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //80 81 分支分润清算日期 合作伙伴清算日期
    {
        //data_union_st->trace_st.branch_clear_date    = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.branch_clear_date    = atoi(data_union_st->file_acoma_st.f_file_date);
        //data_union_st->trace_st.partner_clear_date   = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.partner_clear_date   = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //82. 差错标志    0：正常 1拒付 2调单 见差错流程表
    {
        data_union_st->trace_st.error_point[0]  = '0';
    }

    //83. 风险等级    0：正常，其他见几级风险规则
    {
        data_union_st->trace_st.risk_flag[0] = '0';
    }

    //84. MAC押码
    {
        strcpy (data_union_st->trace_st.mac, "FFFFFFFF");
    }

    return NOERR;
}

int db_insert_into_trace(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret ;
    char sql[8024], sql_info[512];
    DB_GET_ST data [] = {
        "SYSTEM_REF"              ,    &data_union_st->trace_st.system_ref             ,  "SYSTEM_REF         "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.system_ref            ),
        "ORDER_NO"                ,     data_union_st->trace_st.order_no               ,  "ORDER_NO           "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.order_no            ),
        "TRACE_INDEX1"            ,     data_union_st->trace_st.trace_index1           ,  "TRACE_INDEX1       "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trace_index1        ),
        "TRACE_INDEX2"            ,     data_union_st->trace_st.trace_index2           ,  "TRACE_INDEX2       "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trace_index2        ),
        "ORG_SYSTEMREF"           ,    &data_union_st->trace_st.org_systemref          ,  "ORG_SYSTEMREF      "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.org_systemref        ),
        "ORG_MESG"                ,     data_union_st->trace_st.org_mesg               ,  "ORG_MESG           "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.org_mesg              ),
        "TRACE_ORGINDEX1"         ,     data_union_st->trace_st.trace_orgindex1        ,  "TRACE_ORGINDEX1    "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trace_orgindex1        ),
        "ACCT_TRACE_BEGIN"        ,    &data_union_st->trace_st.acct_trace_begin       ,  "ACCT_TRACE_BEGIN   "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.acct_trace_begin    ),
        "CUT_DATE"                ,    &data_union_st->trace_st.cut_date               ,  "CUT_DATE           "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.cut_date                ),
        "SYSTEM_DATE"             ,    &data_union_st->trace_st.system_date            ,  "SYSTEM_DATE        "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.system_date            ),
        "SYSTEM_TIME"             ,    &data_union_st->trace_st.system_time            ,  "SYSTEM_TIME        "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.system_time            ),
        "BRANCH_MAP_ID"           ,    &data_union_st->trace_st.branch_map_id          ,  "BRANCH_MAP_ID      "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.branch_map_id            ),
        "BRANCH_CODE"             ,    &data_union_st->trace_st.branch_code            ,  "BRANCH_CODE        "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.branch_code            ),
        "PARTNER_MAP_ID"          ,    &data_union_st->trace_st.partner_map_id         ,  "PARTNER_MAP_ID     "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.partner_map_id            ),
        "PARTNER_CODE"            ,    &data_union_st->trace_st.partner_code           ,  "PARTNER_CODE       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.partner_code        ),
        "TRANS_ID"                ,    &data_union_st->trace_st.trans_id               ,  "TRANS_ID           "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.trans_id            ),
        "TRANS_SUBID"             ,    &data_union_st->trace_st.trans_subid            ,  "TRANS_SUBID        "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.trans_subid            ),
        "TRANS_SUBNAME"           ,     data_union_st->trace_st.trans_subname          ,  "TRANS_SUBNAME      "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_subname        ),
        "TRANS_TYPE"              ,     data_union_st->trace_st.trans_type             ,  "TRANS_TYPE         "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_type            ),
        "TRANS_RETCODE"           ,     data_union_st->trace_st.trans_retcode          ,  "TRANS_RETCODE      "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_retcode        ),
        "TRANS_RETDESC"           ,     data_union_st->trace_st.trans_retdesc          ,  "TRANS_RETDESC      "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_retdesc        ),
        "TRANS_STATUS"            ,     data_union_st->trace_st.trans_status           ,  "TRANS_STATUS       "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_status        ),
        "TRANS_VOIDFLAG"          ,     data_union_st->trace_st.trans_voidflag         ,  "TRANS_VOIDFLAG     "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_voidflag        ),
        "TRANS_REVERFLAG"         ,     data_union_st->trace_st.trans_reverflag        ,  "TRANS_REVERFLAG    "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_reverflag        ),
        "TRANS_REFUNDFLAG"        ,     data_union_st->trace_st.trans_refundflag       ,  "TRANS_REFUNDFLAG   "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_refundflag    ),
        "TRANS_IC"                ,     data_union_st->trace_st.trans_ic               ,  "TRANS_IC           "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.trans_ic            ),
        "SHOP_NO"                 ,     data_union_st->trace_st.shop_no                ,  "SHOP_NO            "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.shop_no                ),
        "SHOP_NAMEAB"             ,     data_union_st->trace_st.shop_nameab            ,  "SHOP_NAMEAB        "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.shop_nameab            ),
        "COUNTY_ID"               ,    &data_union_st->trace_st.county_id              ,  "COUNTY_ID          "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.county_id                ),
        "POS_NO"                  ,     data_union_st->trace_st.pos_no                 ,  "POS_NO             "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.pos_no                ),
        "POS_MODE"                ,     data_union_st->trace_st.pos_mode               ,  "POS_MODE           "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.pos_mode            ),
        "CARD_SN"                 ,     data_union_st->trace_st.card_sn                ,  "CARD_SN            "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.card_sn                ),
        "POS_SERVICE"             ,     data_union_st->trace_st.pos_service            ,  "POS_SERVICE        "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.pos_service            ),
        "POS_PINCAP"              ,     data_union_st->trace_st.pos_pincap             ,  "POS_PINCAP         "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.pos_pincap            ),
        "PROC_CODE"               ,     data_union_st->trace_st.proc_code              ,  "PROC_CODE          "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.proc_code            ),
        "POS_BATCH"               ,     data_union_st->trace_st.pos_batch              ,  "POS_BATCH          "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.pos_batch            ),
        "POS_TRACE"               ,     data_union_st->trace_st.pos_trace              ,  "POS_TRACE          "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.pos_trace            ),
        "POS_DATE"                ,    &data_union_st->trace_st.pos_date               ,  "POS_DATE           "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.pos_date            ),
        "POS_TIME"                ,    &data_union_st->trace_st.pos_time               ,  "POS_TIME           "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.pos_time            ),
        "HOST_MAP_ID"             ,    &data_union_st->trace_st.host_map_id            ,  "HOST_MAP_ID        "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.host_map_id            ),
        "HOST_ACQ_CODE"           ,     data_union_st->trace_st.host_acq_code          ,  "HOST_ACQ_CODE      "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.host_acq_code        ),
        "HOST_TRACE"              ,     data_union_st->trace_st.host_trace             ,  "HOST_TRACE         "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.host_trace            ),
        "HOST_AUTH"               ,     data_union_st->trace_st.host_auth              ,  "HOST_AUTH          "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.host_auth            ),
        "HSHOP_NO"                ,     data_union_st->trace_st.hshop_no               ,  "HSHOP_NO           "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.hshop_no            ),
        "HSHOP_NAMEAB"            ,     data_union_st->trace_st.hshop_nameab           ,  "HSHOP_NAMEAB       "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.hshop_nameab        ),
        "HPOS_NO"                 ,     data_union_st->trace_st.hpos_no                ,  "HPOS_NO            "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.hpos_no                ),
        "HPOS_BATCH"              ,     data_union_st->trace_st.hpos_batch             ,  "HPOS_BATCH         "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.hpos_batch            ),
        "HPOS_TRACE"              ,     data_union_st->trace_st.hpos_trace             ,  "HPOS_TRACE         "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.hpos_trace            ),
        "CARD_BANK_ID"            ,     data_union_st->trace_st.card_bank_id           ,  "CARD_BANK_ID       "    , PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->trace_st.card_bank_id        ),
        "CARD_BANK_NAME"          ,     data_union_st->trace_st.card_bank_name         ,  "CARD_BANK_NAME     "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.card_bank_name        ),
        "CARD_NO"                 ,     data_union_st->trace_st.card_no                ,  "CARD_NO            "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.card_no                ),
        "CARD_EXP"                ,     data_union_st->trace_st.card_exp               ,  "CARD_EXP           "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.card_exp            ),
        "CARD_FLAG"               ,     data_union_st->trace_st.card_flag              ,  "CARD_FLAG          "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.card_flag            ),
        "PAY_TYPE"                ,    &data_union_st->trace_st.pay_type               ,  "PAY_TYPE           "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.pay_type            ),
        "TRANS_AMT"               ,    &data_union_st->trace_st.trans_amt              ,  "TRANS_AMT          "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.trans_amt            ),
        "TRANS_REFAMT"            ,    &data_union_st->trace_st.trans_refamt           ,  "TRANS_REFAMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.trans_refamt                ),
        "SHOP_NET_AMT"            ,    &data_union_st->trace_st.shop_net_amt           ,  "SHOP_NET_AMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.shop_net_amt        ),
        "SHOP_FEE_AMT"            ,    &data_union_st->trace_st.shop_fee_amt           ,  "SHOP_FEE_AMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.shop_fee_amt        ),
        "SHOP_MDR_AMT"            ,    &data_union_st->trace_st.shop_mdr_amt           ,  "SHOP_MDR_AMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.shop_mdr_amt        ),
        "SHOP_AVR_AMT"            ,    &data_union_st->trace_st.shop_avr_amt           ,  "SHOP_AVR_AMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.shop_avr_amt        ),
        "MDR_HOST_AMT"            ,    &data_union_st->trace_st.mdr_host_amt           ,  "MDR_HOST_AMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.mdr_host_amt        ),
        "MDR_ISS_AMT"             ,    &data_union_st->trace_st.mdr_iss_amt            ,  "MDR_ISS_AMT        "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.mdr_iss_amt            ),
        "MDR_CUP_AMT"             ,    &data_union_st->trace_st.mdr_cup_amt            ,  "MDR_CUP_AMT        "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.mdr_cup_amt            ),
        "MDR_LOGO_AMT"            ,    &data_union_st->trace_st.mdr_logo_amt           ,  "MDR_LOGO_AMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.mdr_logo_amt            ),
        "MDR_PLAT_AMT"            ,    &data_union_st->trace_st.mdr_plat_amt           ,  "MDR_PLAT_AMT       "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.mdr_plat_amt        ),
        "MDR_PARTNER_AMT"         ,    &data_union_st->trace_st.mdr_partner_amt        ,  "MDR_PARTNER_AMT    "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.mdr_partner_amt     ),
        "SETTLE_POS_FLAG"         ,     data_union_st->trace_st.settle_pos_flag        ,  "SETTLE_POS_FLAG    "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.settle_pos_flag        ),
        "SETTLE_HOST_FLAG"        ,     data_union_st->trace_st.settle_host_flag       ,  "SETTLE_HOST_FLAG   "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.settle_host_flag    ),
        "SETTLE_PLAT_AMT"         ,    &data_union_st->trace_st.settle_plat_amt        ,  "SETTLE_PLAT_AMT    "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.settle_plat_amt        ),
        "SETTLE_HOST_AMT"         ,    &data_union_st->trace_st.settle_host_amt        ,  "SETTLE_HOST_AMT    "    , PTS_DB_TYPE_LONG,   sizeof(data_union_st->trace_st.settle_host_amt        ),
        "HOST_CLEAR_TYPE"         ,     data_union_st->trace_st.host_clear_type        ,  "HOST_CLEAR_TYPE    "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.host_clear_type        ),
        "HOST_CLEAR_DATE"         ,    &data_union_st->trace_st.host_clear_date        ,  "HOST_CLEAR_DATE    "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.host_clear_date        ),
        "SHOP_CLEAR_TYPE"         ,     data_union_st->trace_st.shop_clear_type        ,  "SHOP_CLEAR_TYPE    "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.shop_clear_type        ),
        "SHOP_CLEAR_DATE"         ,    &data_union_st->trace_st.shop_clear_date        ,  "SHOP_CLEAR_DATE    "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.shop_clear_date        ),
        "BRANCH_CLEAR_DATE"       ,    &data_union_st->trace_st.branch_clear_date      ,  "BRANCH_CLEAR_DATE  "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.branch_clear_date    ),
        "PARTNER_CLEAR_DATE"      ,    &data_union_st->trace_st.partner_clear_date     ,  "PARTNER_CLEAR_DATE "    , PTS_DB_TYPE_INT ,   sizeof(data_union_st->trace_st.partner_clear_date  ),
        "ERROR_POINT"             ,     data_union_st->trace_st.error_point            ,  "ERROR_POINT        "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.error_point            ),
        "RISK_FLAG"               ,     data_union_st->trace_st.risk_flag              ,  "RISK_FLAG          "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.risk_flag            ),
        "MAC"                     ,     data_union_st->trace_st.mac                    ,  "MAC                "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.mac                    ),
        "RES"                     ,     data_union_st->trace_st.res                    ,  "RES                "    , PTS_DB_TYPE_CHAR,   sizeof(data_union_st->trace_st.res                    ),
        ""                        ,     NULL                                           ,  NULL                    , 0               ,   0 };

    memset (sql        , 0, sizeof(sql));
    strcpy (sql_info   , "INSERT A_TRACE");
    sprintf(sql        , "INSERT INTO %s.A_TRACE SET ", db_posp_name);

    ret = db_insert_one (data, conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
        return (ret);
    }
    
    return NOERR;
}

int db_select_exist_trace(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret ;
    char sql[1024], sql_info[512];

    memset (sql        , 0, sizeof(sql));
    strcpy (sql_info   , "SELECT A_TRACE.EXSIT");
    sprintf(sql ,
           "SELECT * "
           "  FROM %s.A_TRACE "
           " WHERE ORG_MESG = \'%s\' "
           , db_posp_name
           , data_union_st->cur_index_str
    );

    ret = db_exsit_check (conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
    }
    
    return (ret);
}

int db_select_exist_zpsum(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret ;
    char sql[1024], sql_info[512];

    memset (sql        , 0, sizeof(sql));
    strcpy (sql_info   , "SELECT T_LIQUIDATE_FILE_ZPSUM.EXSIT");
    sprintf(sql ,
           "SELECT * "
           "  FROM t_liquidate_file_zpsum t "
           " WHERE t.f_file_date = \'%s\' "
           "   AND t.f_merchant_no = \'%s\' "
           "   AND t.f_bank_acct_no is not null "
           , data_union_st->file_acoma_st.f_file_date
           , data_union_st->file_acoma_st.f_merchant_no
    );

    ret = db_exsit_check (conn_ptr, sql, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        db_rollback (conn_ptr);
    }
    
    return (ret);
}

/**
 *对账控制函数
 **/
int insert_exec(MYSQL *conn_ptr)
{
    DATA_UNION_ST   data_union_st;
    DB_RESULT       *db_result = NULL;
    int             ret, i ;
    char            sql[1024]     = { 0 };
    char            sql_info[512] = { 0 };

    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));

    /*准备游标*/
    strcpy (sql_info    , "SELECT T_LIQUIDATE_FILE_ACOMA");
    sprintf (sql,
            "SELECT * "
            "  FROM T_LIQUIDATE_FILE_ACOMA T "
            " WHERE T.F_FILE_DATE = \'%s\' "
            "   AND T.F_CHANNEL_NO = %d "
            " ORDER BY T.F_TRAN_TIME ASC "
            , szDailyDate
            , CHANNEL_CUP_DIRECT
    );

    /*执行游标*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "需要处理的流水笔数[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        memset( &data_union_st, 0x00, sizeof( DATA_UNION_ST ) );
        /*获取流水数据*/
        ret = db_get_file_acoma_info(db_result, &data_union_st, sql_info);
        if (ret == -2)
        {
            SysLog( LOGTYPE_INFO , "遍历结束" );
            break;
        }
        
        /*系统流水重复校验*/
        ret = db_select_exist_trace(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        else
        if (ret > 0)
        {
            //已登记过，不再登记
            continue;
        }
        
        /*通过zpsum文件拆分银联入账和收单入账*/
        ret = db_select_exist_zpsum(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        else
        if (ret > 0)
        {
            //直连银联代理清算
            sprintf( data_union_st.shop_clear_mode, "%d", SHOP_STL_MODE_FUND ); //代理清算
        }
        else
        {
            //直连收单机构清算
            sprintf( data_union_st.shop_clear_mode, "%d", SHOP_STL_MODE_ACQ ); //自主清算
        }
        
        /*获取卡bin数据*/
        ret = db_get_cardbin_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            SysLog( LOGTYPE_ERROR , "[%s]未匹配到卡bin", data_union_st.file_acoma_st.f_card_no );
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        ret = db_get_trantype_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            SysLog( LOGTYPE_ERROR , "[%s][%s][%s]未匹配到交易类型"
                   , data_union_st.file_acoma_st.f_msg_code
                   , data_union_st.file_acoma_st.f_tran_code
                   , data_union_st.file_acoma_st.f_sub_tran_code
            );
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*获取卡bin数据*/
        ret = db_get_file_lfee_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            SysLog( LOGTYPE_ERROR , "此交易[%s-%s]无品牌服务费", data_union_st.file_acoma_st.f_tran_time, data_union_st.file_acoma_st.f_trace_no );
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*获取城市信息*/
        ret = db_get_city_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*撤销和冲正获取原交易数据*/
        if (atoi(data_union_st.tran_type_st.f_tran_type) == APP_CANCEL 
        	  || atoi(data_union_st.tran_type_st.f_tran_type) == APP_REFUND )
        {
            ret = db_get_org_info(conn_ptr, &data_union_st);
            if (ret < 0 )
            {
                db_rollback (conn_ptr);
                db_free_result(db_result);
                return (ret);
            }
        }
        else if ( atoi(data_union_st.tran_type_st.f_tran_type) == APP_AUTOVOID )
        {
            ret = db_get_org_info(conn_ptr, &data_union_st);
            if (ret < 0 )
            {
                db_rollback (conn_ptr);
                db_free_result(db_result);
                return (ret);
            }
            continue;
        }
        
        /*获取流水数据*/
        ret = db_get_sys_info(conn_ptr, &data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*准备流水数据*/
        ret = func_prepare_trace_st(&data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*登记系统流水*/
        ret = db_insert_into_trace(conn_ptr, &data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
    }
    
    db_free_result(db_result);
    
    return NOERR;
}

/**
 *对账结束
 **/
int insert_init(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "登记流水开始……" );
    
    get_dayend_date( szDailyDate );
    strcpy( db_posp_name, getenv("MYSQL_POSP_NAME") );
    
    return NOERR;
}

/**
 *对账结束
 **/
int insert_end(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "登记流水结束……" );
    
    return NOERR;
}

/**
 *入口函数
 **/
int func_insert_trace(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    /*登记流水初始化*/
    iRet = insert_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "登记流水初始化失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*登记流水*/
    iRet = insert_exec(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "登记流水失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*登记流水结束*/
    iRet = insert_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "登记流水结束失败" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    return STEP_EXEC_STATUS_SUCC;
}