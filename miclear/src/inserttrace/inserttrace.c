/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������ϵͳ
 *    SystemName  : ���Ȳ���-�Ǽ�ֱ���̻�������ˮ����ˮ��
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-�Ǽ�ֱ���̻�������ˮ����ˮ��
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/03/09         ����           �����         �����ĵ�
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"
#include "dbbase.h"

/*������������*/
#define MAX_DEFF_DAYS 2

#define LOG_SQL_TITLE "�Ǽ�ϵͳ��ˮ"

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
    long    system_ref                    ; //    ϵͳ��ˮ��    ��Ψһ������A_SYS
    char    order_no            [36 + 1]; //    ������    ��Ψһ�������������������Ų�0��32λ+����ID��0��4λ����Ԥ����ͨ����������ˮ�Ų�0��32λ+����ID��0��4λ
    char    trace_index1        [35 + 1]; //    �����̻�����    �̻��ն�������ˮ��Ψһ������
    char    trace_index2        [58 + 1]; //
    long    org_systemref                ; //    ԭ����ϵͳ��ˮ��    ����Ϊ0
    char    org_mesg            [42 + 1]; //
    char    trace_orgindex1        [35 + 1]; //    ԭʼ��ˮ��������    ��������INDEX1����
    long    acct_trace_begin            ; //    ��¼�˻���ˮ��
    int     cut_date                    ; //    ��������
    int     system_date                    ; //    ϵͳ����
    int     system_time                    ; //    ϵͳʱ��
    int     branch_map_id               ;
    long    branch_code                    ; //    ��֧����    ���������̻�����
    int     partner_map_id              ;
    long    partner_code                ; //    ��������    ������������Ϊ0
    int     trans_id                    ; //    ��������
    int     trans_subid                    ; //    �ӽ�������
    char    trans_subname        [40 + 1]; //    ��������
    char    trans_type            [1  + 1]; //    ����ʽ    0��ϵͳ  1���ֹ�
    char    trans_retcode        [2  + 1]; //    ������
    char    trans_retdesc        [40 + 1]; //    ����˵��
    char    trans_status        [1  + 1]; //    ��ˮ��־    0���ɹ� 1ʧ�� 9������
    char    trans_voidflag        [1  + 1]; //    ���ױ�ʶ    0���� 1����
    char    trans_reverflag        [1  + 1]; //    ���ױ�ʶ    0���� 1����
    char    trans_refundflag    [1  + 1]; //    ���ױ�ʶ    0���� 1�˻�
    char    trans_ic            [510+ 1]; //    IC��ϸ����
    char    shop_no                [15 + 1]; //    �̻���    ��������
    char    shop_nameab            [60 + 1]; //    �̻�����    ����
    int     county_id                   ;
    char    pos_no                [8  + 1]; //    POS��    ����
    char    pos_mode            [4  + 1]; //    ����ģʽ
    char    card_sn             [3  + 1];
    char    pos_service         [2  + 1];
    char    pos_pincap          [2  + 1];
    char    proc_code           [6  + 1];
    char    pos_batch            [6  + 1]; //    POS���κ�
    char    pos_trace            [6  + 1]; //    POS��ˮ��
    int     pos_date                    ; //    POS��������    ��������
    int     pos_time                    ; //    POS����ʱ��
    int     host_map_id                    ; //    ·������    ��������+HOST_TRACE���Ψһ
    char    host_acq_code        [11 + 1]; //    ��������    ����Ϊȫ0
    char    host_trace            [12 + 1]; //    ������ˮ
    char    host_auth            [6  + 1]; //    ��Ȩ��
    char    hshop_no            [15 + 1]; //    �̻���    ����
    char    hshop_nameab        [60 + 1]; //    �̻�����    ����
    char    hpos_no                [8  + 1]; //    POS��    ����
    char    hpos_batch            [6  + 1]; //    POS���κ�
    char    hpos_trace            [6  + 1]; //    POS��ˮ��
    char    card_bank_id          [20+1]; //    ���д���
    char    card_bank_name        [60 + 1]; //    ��������
    char    card_no                [19 + 1]; //    ����    ��������
    char    card_exp            [4  + 1]; //    ����Ч��
    char    card_flag            [1  + 1]; //    ����־    ���������ֽ���ǿ���
    int     pay_type                    ; //    ֧����ʽ    ����������A_PAYTYPE��
    long    trans_amt                    ; //    ���׽��
    long    trans_refamt                ; //    ���׽��
    long    shop_net_amt                ; //    ���׾����    �̻���
    long    shop_fee_amt                ; //    ������������    Ϊ�̻��������ܺ�=�յ�+��ֵ
    long    shop_mdr_amt                ; //    �յ�������    =ͨ���յ��ɱ�+ƽ̨�յ�����
    long    shop_avr_amt                ; //    ��ֵ������    Ԥ�� 0 = ��ֵ��Ա����+��ֵƽ̨����
    long    mdr_host_amt                ; //    ͨ���յ��ɱ����
    long    mdr_iss_amt                    ; //    ͨ���յ��ɱ����
    long    mdr_cup_amt                    ; //    ͨ���յ��ɱ����
    long    mdr_logo_amt                ; //    ͨ���յ��ɱ����
    long    mdr_plat_amt                ; //    ƽ̨�յ�������
    long    mdr_partner_amt             ;
    char    settle_pos_flag        [1  + 1]; //    POS�����־    0��ƽ�� 1:�ն˶� 2:ƽ̨�� 9�쳣��0
    char    settle_host_flag    [1  + 1]; //    ͨ�����˱�־    0��ƽ�� 1��׷�� 2������ 3������ 9���쳣
    long    settle_plat_amt                ; //    ͨ��Ӧ��������    ƽ̨
    long    settle_host_amt                ; //    ͨ��ʵ��������    ͨ��/������ʼֵͬ
    char    host_clear_type        [1  + 1]; //    ͨ�����㷽ʽ    0���յ�ģʽ�����壩 1������ģʽ
    int     host_clear_date                ; //    ͨ����������    ��������
    char    shop_clear_type        [1  + 1]; //    �̻����㷽ʽ    0���̻�ֱ��1ͨ��ֱ�ӵ��̻� 2�������̻� 3ƽ̨���̻� 4ƽ̨�������̻� 5ƽ̨������
    int     shop_clear_date                ; //    �̻���������    ��������
    int     branch_clear_date            ; //    ��֧������������    ��������
    int     partner_clear_date            ; //    ��֧������������    ��������
    char    error_point            [1  + 1]; //    ����־    0������ 1�ܸ� 2���� ��������̱�
    char    risk_flag            [1  + 1]; //    ���յȼ�    0���������������������չ���
    char    mac                    [32 + 1]; //    MACѺ��    ���н��
    char    res                    [90 + 1]; //   ��ע    �ɱ��������Ϣ
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
    char            org_index_str[42+1];  /*ƥ��ԭ���׵ļ�������*/
    char            cur_index_str[42+1];  /*ƥ��ԭ���׵ļ�������*/
    char            shop_clear_mode[1+1]; /*�̻�����ģʽ 1-ͨ�����̻� 3-ƽ̨���̻�*/
}DATA_UNION_ST;

/**
 *��ѯϵͳ�ο���
 **/
int db_get_sys_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st )
{
    int ret ;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "SYSTEM_STATUS",  data_union_st->sys_st.system_status, "ϵͳ״ֵ̬", PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->sys_st.system_status),
        "SYSTEM_BATCH" , &data_union_st->sys_st.system_batch , "ϵͳ���κ�", PTS_DB_TYPE_LONG ,  sizeof(data_union_st->sys_st.system_batch ),
        "SYSTEM_REF"   , &data_union_st->sys_st.system_ref   , "�����ο���", PTS_DB_TYPE_LONG ,  sizeof(data_union_st->sys_st.system_ref   ),
        "SETTLE_DATE"  ,  data_union_st->sys_st.settle_date  , "�ϴ�������", PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->sys_st.settle_date  ),
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
 *��ȡƷ�Ʒ����
 **/
int db_get_file_lfee_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "f_id"               , &data_union_st->file_lfee_st.f_id                 , "ҵ��id                      ",   PTS_DB_TYPE_LONG ,  sizeof(data_union_st->file_lfee_st.f_id    ),
        "f_file_date"        , data_union_st->file_lfee_st.f_file_date           , "�ļ�����                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_file_date  ),
        "f_channel_no"       , &data_union_st->file_lfee_st.f_channel_no         , "ͨ�����                    " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_lfee_st.f_channel_no    ),
        "f_agent_code"       , data_union_st->file_lfee_st.f_agent_code          , "����������루32��        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_agent_code    ),
        "f_snd_code"         , data_union_st->file_lfee_st.f_snd_code            , "���ͻ�����33��            " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_snd_code    ),
        "f_rcv_code"         , data_union_st->file_lfee_st.f_rcv_code            , "���ܻ�����100��           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_rcv_code     ),
        "f_iss_code"         , data_union_st->file_lfee_st.f_iss_code            , "������������                " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->file_lfee_st.f_iss_code   ),
        "f_trace_no"         , data_union_st->file_lfee_st.f_trace_no            , "ϵͳ���ٺţ�11��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_trace_no   ),
        "f_tran_time"        , data_union_st->file_lfee_st.f_tran_time           , "����ʱ�䣨7��             " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_tran_time),
        "f_card_no"          , data_union_st->file_lfee_st.f_card_no             , "���ţ�2��                 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_card_no),
        "f_card_type"        , data_union_st->file_lfee_st.f_card_type           , "������                      " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_card_type),
        "f_tran_amt"         , &data_union_st->file_lfee_st.f_tran_amt           , "���׽��                    " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_tran_amt),
        "f_tran_type"        , &data_union_st->file_lfee_st.f_tran_type          , "ϵͳ��������                " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_lfee_st.f_tran_type),
        "f_mcc"              , data_union_st->file_lfee_st.f_mcc                 , "mcc                         " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_mcc),
        "f_merchant_no"      , data_union_st->file_lfee_st.f_merchant_no         , "�̻��ţ�42��              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_merchant_no),
        "f_terminal_no"      , data_union_st->file_lfee_st.f_terminal_no         , "�ն˺ţ�41��              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_terminal_no),
        "f_tran_rrn"         , data_union_st->file_lfee_st.f_tran_rrn            , "���ײο��ţ�37��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_tran_rrn),
        //"f_auth_no"          , data_union_st->file_lfee_st.f_auth_no             , "��ȨӦ���루38��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_auth_no),
        "f_msg_code"         , data_union_st->file_lfee_st.f_msg_code            , "��������                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_msg_code),
        "f_tran_code"        , data_union_st->file_lfee_st.f_tran_code           , "���������루3��           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_tran_code),
        "f_sub_tran_code"    , data_union_st->file_lfee_st.f_sub_tran_code       , "����������루25��        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_sub_tran_code),
        //"f_resp_code"        , data_union_st->file_lfee_st.f_resp_code           , "���׷����루39��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_resp_code),
        //"f_pos_mode"         , data_union_st->file_lfee_st.f_pos_mode            , "��������뷽ʽ              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_pos_mode),
        "f_acq_fee"          , &data_union_st->file_lfee_st.f_acq_fee            , "����Ӧ��������            " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_acq_fee),
        "f_lfee"             , &data_union_st->file_lfee_st.f_lfee               , "Ʒ�Ʒ����                  " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_lfee),
        "f_net_amt"          , &data_union_st->file_lfee_st.f_net_amt            , "���׾���                    " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_net_amt),
        "f_org_info"         , data_union_st->file_lfee_st.f_org_info            , "ԭ������Ϣ                  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_org_info),
        //"f_org_trace_no"     , data_union_st->file_lfee_st.f_org_trace_no        , "ԭʼ����ϵͳ���ٺţ�90.2��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_org_trace_no),
        //"f_org_tran_time"    , data_union_st->file_lfee_st.f_org_tran_time       , "ԭʼ��������ʱ�䣨90.3��  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_org_tran_time),
        "f_data_lfee "       , data_union_st->file_lfee_st.f_data_lfee           , "Ʒ�Ʒ�����ļ�һ����        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_data_lfee),
        "f_comments"         , data_union_st->file_lfee_st.f_comments            , "��ע                        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_comments),
        //"f_reversal_flag"    , data_union_st->file_lfee_st.f_reversal_flag       , "������ʶ R-�ѳ��� �ո�-���� " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_reversal_flag),
        //"f_cancle_flag"      , data_union_st->file_lfee_st.f_cancle_flag         , "������ʶ C-�ѳ��� �ո�-���� " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_cancle_flag),
        //"f_clear_date"       , data_union_st->file_lfee_st.f_clear_date          , "��������                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_clear_date),
        //"f_merchant_name_add", data_union_st->file_lfee_st.f_merchant_name_add   , "�̻����Ƶ�ַ                " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_lfee_st.f_merchant_name_add),
        //"f_merchant_fee"     , &data_union_st->file_lfee_st.f_merchant_fee       , "�̻�������                  " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_lfee_st.f_merchant_fee),
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
 *��ȡԭ������Ϣ
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
        //��ǰ��ˮ�鲻��������ʷ��ˮ
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
 *��ȡ��bin��Ϣ
 **/
int db_get_cardbin_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "CARD_BIN"       , &data_union_st->cardbin_st.card_bin       , "��BIN ��",   PTS_DB_TYPE_LONG ,  sizeof(data_union_st->cardbin_st.card_bin    ),
        "CARD_NAME"      ,  data_union_st->cardbin_st.card_name      , "��������" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->cardbin_st.card_name   ),
        "CARD_TRACK"     , &data_union_st->cardbin_st.card_track     , "���ڴŵ�" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_track  ),
        "CARD_OFF"       , &data_union_st->cardbin_st.card_off       , "ƫ��λ��" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_off    ),
        "CARD_LEN"       , &data_union_st->cardbin_st.card_len       , "���ų���" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_len    ),
        "PAY_TYPE"       , &data_union_st->cardbin_st.pay_type       , "֧������" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.pay_type    ),
        "BANK_ID"        ,  data_union_st->cardbin_st.bank_id        , "���д���" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->cardbin_st.bank_id     ),
        "BANK_NAME"      ,  data_union_st->cardbin_st.bank_name      , "��������" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->cardbin_st.bank_name   ),
        "CARD_FLAG"      , &data_union_st->cardbin_st.card_flag      , "�����־" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.card_flag   ),
        "ROUTE_MAP_ID"   , &data_union_st->cardbin_st.route_map_id   , "Ĭ��·��" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->cardbin_st.route_map_id),
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
 *��ȡ��������
 **/
int db_get_trantype_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "f_id"                , &data_union_st->tran_type_st.f_id             , "id                      " ,   PTS_DB_TYPE_INT ,  sizeof(data_union_st->tran_type_st.f_id    ),
        "f_tran_type"         ,  data_union_st->tran_type_st.f_tran_type      , "ϵͳ��������            " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->tran_type_st.f_tran_type   ),
        "f_tran_sub_type"     , data_union_st->tran_type_st.f_tran_sub_type   , "ϵͳ������������        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_tran_sub_type  ),
        "f_tran_name"         , data_union_st->tran_type_st.f_tran_name       , "ϵͳ��������            " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_tran_name    ),
        "f_mesg_code"         , data_union_st->tran_type_st.f_mesg_code       , "��Ϣ������              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_mesg_code    ),
        "f_tran_code"         , data_union_st->tran_type_st.f_tran_code       , "���������루3��       " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_tran_code    ),
        "f_sub_tran_code"     , data_union_st->tran_type_st.f_sub_tran_code   , "����������루25��    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->tran_type_st.f_sub_tran_code     ),
        "f_liquidate_flag"    ,  data_union_st->tran_type_st.f_liquidate_flag , "�����ʶ D-��� C-����  " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->tran_type_st.f_liquidate_flag   ),
        "f_org_mesg_code"     ,  data_union_st->tran_type_st.f_org_mesg_code  , "ԭʼ��������            " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->tran_type_st.f_org_mesg_code   ),
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
    
    //��������Ϊ 32 33 7 11���Ψһ��ƥ��ԭ����
    sprintf( data_union_st->org_index_str, "%s%s%s000%s000%s"
            , data_union_st->tran_type_st.f_org_mesg_code
            , data_union_st->file_acoma_st.f_org_trace_no
            , data_union_st->file_acoma_st.f_org_tran_time
            , data_union_st->file_acoma_st.f_agent_code
            , data_union_st->file_acoma_st.f_snd_code
    );
    
    /*�����࣬�任���׽��������*/
    if ( data_union_st->tran_type_st.f_liquidate_flag[0] == DC_FLAG_C )
    {
        data_union_st->file_acoma_st.f_tran_amt = -1 * data_union_st->file_acoma_st.f_tran_amt ;
    }
    
    return (0);
}

/**
 *��ȡ���׵�����Ϣ
 **/
int db_get_city_info(MYSQL *conn_ptr, DATA_UNION_ST *data_union_st)
{
    int ret;
    char sql[1024]     = { 0 };
    char sql_info[512] = { 0 };
    
    DB_GET_ST data [] = {
        "COUNTY_ID"       , &data_union_st->city_st.COUNTY_ID       , "����ֵ   ",   PTS_DB_TYPE_INT ,  sizeof(data_union_st->city_st.COUNTY_ID    ),
        "COUNT_NAME"      ,  data_union_st->city_st.COUNT_NAME      , "��������" ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->city_st.COUNT_NAME   ),
        "CITY_NAME"       , data_union_st->city_st.CITY_NAME        , "��������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->city_st.CITY_NAME  ),
        "PROVINCE_NAME"   , data_union_st->city_st.PROVINCE_NAME    , "ʡ������" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->city_st.PROVINCE_NAME    ),
        "TEL_ID"          , &data_union_st->city_st.TEL_ID          , "�绰����" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->city_st.TEL_ID    ),
        "ZIP_ID"          , &data_union_st->city_st.ZIP_ID          , "��������" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->city_st.ZIP_ID    ),
        "CLEAR_CODE"      , &data_union_st->city_st.CLEAR_CODE      , "�������" ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->city_st.CLEAR_CODE     ),
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
 *ȡ���Ǽ���ˮ������
 **/
int db_get_file_acoma_info(DB_RESULT * db_result, DATA_UNION_ST *data_union_st, char *sql_info)
{
    int ret;
    char sql[1024]     = { 0 };

    DB_GET_ST data [] = {
        "f_id"               , &data_union_st->file_acoma_st.f_id                 , "ҵ��id                      ",   PTS_DB_TYPE_LONG ,  sizeof(data_union_st->file_acoma_st.f_id    ),
        "f_file_date"        , data_union_st->file_acoma_st.f_file_date           , "�ļ�����                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_file_date  ),
        "f_channel_no"       , &data_union_st->file_acoma_st.f_channel_no         , "ͨ�����                    " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_acoma_st.f_channel_no    ),
        "f_agent_code"       , data_union_st->file_acoma_st.f_agent_code          , "����������루32��        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_agent_code    ),
        "f_snd_code"         , data_union_st->file_acoma_st.f_snd_code            , "���ͻ�����33��            " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_snd_code    ),
        "f_rcv_code"         , data_union_st->file_acoma_st.f_rcv_code            , "���ܻ�����100��           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_rcv_code     ),
        "f_iss_code"         , data_union_st->file_acoma_st.f_iss_code            , "������������                " ,   PTS_DB_TYPE_CHAR ,  sizeof(data_union_st->file_acoma_st.f_iss_code   ),
        "f_trace_no"         , data_union_st->file_acoma_st.f_trace_no            , "ϵͳ���ٺţ�11��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_trace_no   ),
        "f_tran_time"        , data_union_st->file_acoma_st.f_tran_time           , "����ʱ�䣨7��             " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_tran_time),
        "f_card_no"          , data_union_st->file_acoma_st.f_card_no             , "���ţ�2��                 " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_card_no),
        "f_tran_amt"         , &data_union_st->file_acoma_st.f_tran_amt           , "���׽��                    " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_tran_amt),
        "f_tran_type"        , &data_union_st->file_acoma_st.f_tran_type          , "ϵͳ��������                " ,   PTS_DB_TYPE_INT  ,  sizeof(data_union_st->file_acoma_st.f_tran_type),
        "f_mcc"              , data_union_st->file_acoma_st.f_mcc                 , "mcc                         " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_mcc),
        "f_merchant_no"      , data_union_st->file_acoma_st.f_merchant_no         , "�̻��ţ�42��              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_merchant_no),
        "f_terminal_no"      , data_union_st->file_acoma_st.f_terminal_no         , "�ն˺ţ�41��              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_terminal_no),
        "f_tran_rrn"         , data_union_st->file_acoma_st.f_tran_rrn            , "���ײο��ţ�37��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_tran_rrn),
        "f_auth_no"          , data_union_st->file_acoma_st.f_auth_no             , "��ȨӦ���루38��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_auth_no),
        "f_msg_code"         , data_union_st->file_acoma_st.f_msg_code            , "��������                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_msg_code),
        "f_tran_code"        , data_union_st->file_acoma_st.f_tran_code           , "���������루3��           " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_tran_code),
        "f_sub_tran_code"    , data_union_st->file_acoma_st.f_sub_tran_code       , "����������루25��        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_sub_tran_code),
        "f_resp_code"        , data_union_st->file_acoma_st.f_resp_code           , "���׷����루39��          " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_resp_code),
        "f_pos_mode"         , data_union_st->file_acoma_st.f_pos_mode            , "��������뷽ʽ              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_pos_mode),
        "f_rcv_fee"          , &data_union_st->file_acoma_st.f_rcv_fee            , "����Ӧ��������            " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_rcv_fee),
        "f_pay_fee"          , &data_union_st->file_acoma_st.f_pay_fee            , "����Ӧ��������            " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_pay_fee),
        "f_tran_fee"         , &data_union_st->file_acoma_st.f_tran_fee           , "ת�ӷ���ѣ�X+n11��         " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_tran_fee),
        "f_card_sn"          , data_union_st->file_acoma_st.f_card_sn             , "��Ƭ���к�                  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_card_sn),
        "f_org_trace_no"     , data_union_st->file_acoma_st.f_org_trace_no        , "ԭʼ����ϵͳ���ٺţ�90.2��" ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_org_trace_no),
        "f_org_tran_time"    , data_union_st->file_acoma_st.f_org_tran_time       , "ԭʼ��������ʱ�䣨90.3��  " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_org_tran_time),
        "f_data_acoma"       , data_union_st->file_acoma_st.f_data_acoma          , "�����ļ�һ����              " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_data_acoma),
        "f_comments"         , data_union_st->file_acoma_st.f_comments            , "��ע                        " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_comments),
        "f_reversal_flag"    , data_union_st->file_acoma_st.f_reversal_flag       , "������ʶ R-�ѳ��� �ո�-���� " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_reversal_flag),
        "f_cancle_flag"      , data_union_st->file_acoma_st.f_cancle_flag         , "������ʶ C-�ѳ��� �ո�-���� " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_cancle_flag),
        "f_clear_date"       , data_union_st->file_acoma_st.f_clear_date          , "��������                    " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_clear_date),
        "f_merchant_name_add", data_union_st->file_acoma_st.f_merchant_name_add   , "�̻����Ƶ�ַ                " ,   PTS_DB_TYPE_CHAR  ,  sizeof(data_union_st->file_acoma_st.f_merchant_name_add),
        "f_merchant_fee"     , &data_union_st->file_acoma_st.f_merchant_fee       , "�̻�������                  " ,   PTS_DB_TYPE_LONG  ,  sizeof(data_union_st->file_acoma_st.f_merchant_fee),
        ""                   , NULL                                               , NULL       ,   0                ,  0};

    ret = db_fetch_cursor (data, db_result, sql_info, LOG_SQL_TITLE);
    if (ret < 0)
    {
        return (ret);
    }
    
    //��������Ϊ 32 33 7 11���Ψһ��ƥ�䱾���ף���ֹ�ظ�����
    sprintf( data_union_st->cur_index_str, "%s%s%s000%s000%s"
            , data_union_st->file_acoma_st.f_msg_code
            , data_union_st->file_acoma_st.f_trace_no
            , data_union_st->file_acoma_st.f_tran_time
            , data_union_st->file_acoma_st.f_agent_code
            , data_union_st->file_acoma_st.f_snd_code
    );
    
    return (0);
}

/*׼����ˮ����*/
int func_prepare_trace_st(DATA_UNION_ST *data_union_st)
{
    //0. �������pos���ͽ�����Ϣ�����κŵ�������Ҫ���죩
    //30. POS���κ�
    {
        sprintf( data_union_st->trace_st.pos_trace, "%06ld", data_union_st->sys_st.system_ref % 1000000 );  //ϵͳ���ٺ�
    }
    
    //31. POS��ˮ��
    {
        sprintf( data_union_st->trace_st.pos_batch, "%06ld", data_union_st->sys_st.system_ref / 1000000 );  //ϵͳ���ٺ�
    }
    
    //32. POS��������
    {
        /*������������ڲ���*/
        if ( strncmp( data_union_st->file_acoma_st.f_file_date + 4, data_union_st->file_acoma_st.f_tran_time, 4 ) )
        {
            data_union_st->trace_st.pos_date = _FunDateAdd( data_union_st->file_acoma_st.f_file_date , -1);
        }
        else
        {
            data_union_st->trace_st.pos_date = atoi( data_union_st->file_acoma_st.f_file_date);
        }
    }

    //33. POS����ʱ��
    {
        data_union_st->trace_st.pos_time = atoi( data_union_st->file_acoma_st.f_tran_time+4 );
    }
        
    {
        strcpy( data_union_st->trace_st.shop_no, data_union_st->file_acoma_st.f_merchant_no );
        strcpy( data_union_st->trace_st.pos_no, data_union_st->file_acoma_st.f_terminal_no );
        strcpy( data_union_st->trace_st.trans_retcode, data_union_st->file_acoma_st.f_resp_code );
        strcpy( data_union_st->trace_st.shop_nameab, data_union_st->file_acoma_st.f_merchant_name_add );
    }

    //7. 8 ϵͳ����/ϵͳʱ��
    {
        data_union_st->trace_st.system_date = atoi(data_union_st->file_acoma_st.f_file_date);
        data_union_st->trace_st.system_time = atoi(data_union_st->file_acoma_st.f_tran_time + 4);

        data_union_st->trace_st.cut_date    = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //1. ϵͳ��ˮ��    ��Ψһ������
    {
        data_union_st->trace_st.system_ref = data_union_st->sys_st.system_ref; //
    }

    //2. ������ ��������ҪUPT��
    {
        sprintf(data_union_st->trace_st.order_no     , "%032ld" , data_union_st->sys_st.system_ref); //
        sprintf(data_union_st->trace_st.order_no + 32, "%04d"   , 1); //ͨ�����
    }

    //3. �ڲ��̻�����    �̻��ն�������ˮ��Ψһ������
    {
        memcpy (data_union_st->trace_st.trace_index1+0      , data_union_st->trace_st.shop_no                  , 15);
        memcpy (data_union_st->trace_st.trace_index1+15      , data_union_st->trace_st.pos_no                   ,  8);
        memcpy (data_union_st->trace_st.trace_index1+23      , data_union_st->trace_st.pos_batch         ,  6);
        memcpy (data_union_st->trace_st.trace_index1+29      , data_union_st->trace_st.pos_trace                ,  6);
    }

    //4. �̻�������Ԥ��Ȩ���˻���ר�ã�    �̻��ն�������ˮ��Ψһ������
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
            //��ʼΨһ�Ϳ��ԣ���Ӧ��ʱ���޸ġ�
            memcpy (data_union_st->trace_st.trace_index2+0      , data_union_st->file_acoma_st.f_msg_code             ,  4);
            sprintf (data_union_st->trace_st.trace_index2+4      , "%08ld", data_union_st->trace_st.pos_date);
            memcpy (data_union_st->trace_st.trace_index2+12      , data_union_st->trace_st.shop_no               , 15);
            sprintf (data_union_st->trace_st.trace_index2+27    , "%012ld", data_union_st->trace_st.system_ref);
        }
    }

    //4. ԭ����ϵͳ��ˮ��
    {
        data_union_st->trace_st.org_systemref = data_union_st->org_trace_st.system_ref;
    }

    //4.����ר�� �̻��ն�������ˮ
    {
        //��������Ϊ 32 33 7 11���Ψһ�����Կ����˻�ԭʼ�������
        memcpy (data_union_st->trace_st.org_mesg+0      , data_union_st->file_acoma_st.f_msg_code             ,  4);
        memcpy (data_union_st->trace_st.org_mesg+4      , data_union_st->file_acoma_st.f_trace_no             ,  6);
        memcpy (data_union_st->trace_st.org_mesg+10      , data_union_st->file_acoma_st.f_tran_time        , 10);
        memcpy (data_union_st->trace_st.org_mesg+20      , "000"                                   ,  3);
        memcpy (data_union_st->trace_st.org_mesg+23      , data_union_st->file_acoma_st.f_agent_code          ,  8);
        memcpy (data_union_st->trace_st.org_mesg+31      , "000"                                   ,  3);
        memcpy (data_union_st->trace_st.org_mesg+34      , data_union_st->file_acoma_st.f_snd_code          ,  8);
    }

    //5. ԭʼ��ˮ��������
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

    //6. ��¼�˻���ˮ��
    {
        data_union_st->trace_st.acct_trace_begin = 0;
    }
    
    //11. 12 ��֧���
    {
        data_union_st->trace_st.branch_map_id   = 0;
        data_union_st->trace_st.branch_code     = 0;
    }

    //13. 14 �����������
    {
        data_union_st->trace_st.partner_map_id  = 0;
        data_union_st->trace_st.partner_code    = 0;
    }

    //15. 16. 17. 18 ��������  ����ʽ
    {
        data_union_st->trace_st.trans_id        = atoi(data_union_st->tran_type_st.f_tran_type);
        data_union_st->trace_st.trans_subid     = atoi(data_union_st->tran_type_st.f_tran_sub_type);
        data_union_st->trace_st.trans_type[0]   = '0';
        memcpy (data_union_st->trace_st.trans_subname, data_union_st->tran_type_st.f_tran_name   , 40);
    }

    //19. 20. ���׽��
    {
        if (memcmp (data_union_st->trace_st.trans_retcode, "00", 2) == 0)
            memcpy (data_union_st->trace_st.trans_retdesc    ,  "���׳ɹ�" , 10);
        else
            memcpy (data_union_st->trace_st.trans_retdesc    ,  "����ʧ��" , 10);
    }

    //21 ��ˮ��־ 0���ɹ� 1ʧ�� 9������
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

    //22.23.24 ���ױ�ʶ    0���� 1���� 1���� 1�˻�
    {
        data_union_st->trace_st.trans_voidflag    [0] = '0';
        data_union_st->trace_st.trans_reverflag    [0] = '0';
        data_union_st->trace_st.trans_refundflag    [0] = '0';
    }

    //25. IC��ϸ����
    {
        strcpy( data_union_st->trace_st.trans_ic, " " );   //    IC��ϸ����
    }

    //26. 27  �̻���
    {
        strcpy (data_union_st->trace_st.shop_nameab , data_union_st->trace_st.shop_nameab );
    }
    
    {
        data_union_st->trace_st.county_id = data_union_st->city_st.COUNTY_ID;
    }
    

    // 28 POS ��
    {
    }

    //29. ����ģʽ
    {
        memcpy (data_union_st->trace_st.pos_mode,data_union_st->file_acoma_st.f_pos_mode, 4);
    }

    strcpy (data_union_st->trace_st.card_sn    , data_union_st->file_acoma_st.f_card_sn );
    strcpy (data_union_st->trace_st.pos_service, data_union_st->file_acoma_st.f_sub_tran_code );
    strcpy (data_union_st->trace_st.pos_pincap , "-");
    strcpy (data_union_st->trace_st.proc_code  , data_union_st->file_acoma_st.f_tran_code);
    
    //34. ·������ID �� �̻����㷽ʽ ��
    {
        data_union_st->trace_st.host_map_id    = CHANNEL_CUP_DIRECT    ;  //0-���� 1-�������� 2-����ֱ�� 3-��������
    }

    //35. ��������
    {
        strcpy (data_union_st->trace_st.host_acq_code, data_union_st->file_acoma_st.f_agent_code );
    }

    //36. ������ˮ
    {
        sprintf (data_union_st->trace_st.host_trace, "%012ld", data_union_st->sys_st.system_ref);
    }

    //37. ��Ȩ��
    {
        memcpy (data_union_st->trace_st.host_auth, data_union_st->file_acoma_st.f_auth_no, 6);
    }

    //38. �̻���(����)
    {
        memcpy (data_union_st->trace_st.hshop_no, data_union_st->trace_st.shop_no, 15);
    }

    //39. �̻�����(����)
    {
        memcpy (data_union_st->trace_st.hshop_nameab, data_union_st->trace_st.shop_nameab, 60);
    }

    //40. POS�Ŷ��� /40 POS���κ� /41 POS��ˮ��
    {
        memcpy (data_union_st->trace_st.hpos_no   , data_union_st->trace_st.pos_no, 8);
        strcpy( data_union_st->trace_st.hpos_batch, data_union_st->trace_st.pos_batch );
        strcpy( data_union_st->trace_st.hpos_trace, data_union_st->file_acoma_st.f_trace_no );
    }

    //42. 43. ���С�44 ����/  45��Ч�� /46������
    {
        
        memcpy (data_union_st->trace_st.card_no         ,  data_union_st->file_acoma_st.f_card_no     , 19);
        strcpy (data_union_st->trace_st.card_exp         ,  "-");
        memcpy (data_union_st->trace_st.card_bank_name,  data_union_st->cardbin_st.bank_name, 60);
        strcpy (data_union_st->trace_st.card_bank_id , data_union_st->cardbin_st.bank_id);
        data_union_st->trace_st.card_flag[0]  = data_union_st->cardbin_st.card_flag[0];
    }

    //54. ֧����ʽ
    {
        data_union_st->trace_st.pay_type = data_union_st->cardbin_st.pay_type;
    }

    //55. ���׽��
    {
        data_union_st->trace_st.trans_amt       =  data_union_st->file_acoma_st.f_tran_amt;
        data_union_st->trace_st.trans_refamt       =  data_union_st->file_acoma_st.f_tran_amt;
    }
    
    //57 58 59 60 61 62 63 65 66 67 68 69 70 71 ������
    {
        data_union_st->trace_st.shop_net_amt	     =  data_union_st->trace_st.trans_amt - data_union_st->file_acoma_st.f_merchant_fee; //	���׾����	�̻������̻������
        data_union_st->trace_st.shop_fee_amt	     =  data_union_st->file_acoma_st.f_merchant_fee; //	������������	Ϊ�̻��������ܺ�=�յ�+��ֵ
        data_union_st->trace_st.shop_mdr_amt	     =  data_union_st->file_acoma_st.f_merchant_fee; //	�յ�������	=ͨ���յ��ɱ�+ƽ̨�յ�����
        data_union_st->trace_st.shop_avr_amt	     =  0; //	��ֵ������	Ԥ�� 0 = ��ֵ��Ա����+��ֵƽ̨����

        data_union_st->trace_st.mdr_host_amt      =  data_union_st->file_acoma_st.f_merchant_fee - (data_union_st->file_acoma_st.f_rcv_fee - data_union_st->file_acoma_st.f_pay_fee)     ; //ͨ���ɱ����̻�������-�յ����棩
        data_union_st->trace_st.mdr_iss_amt       =  data_union_st->file_acoma_st.f_merchant_fee - (data_union_st->file_acoma_st.f_rcv_fee - data_union_st->file_acoma_st.f_pay_fee) - data_union_st->file_acoma_st.f_tran_fee     ; //������=�̻�������-�յ����� - ת��
        data_union_st->trace_st.mdr_cup_amt       =  data_union_st->file_acoma_st.f_tran_fee       ;
        //data_union_st->trace_st.mdr_logo_amt      =  0      ;
        data_union_st->trace_st.mdr_partner_amt   =  0   ;
        data_union_st->trace_st.mdr_plat_amt	    =  (data_union_st->file_acoma_st.f_rcv_fee - data_union_st->file_acoma_st.f_pay_fee)     ;
        //�����յ��������
        if ( atoi(data_union_st->shop_clear_mode) == SHOP_STL_MODE_ACQ ) //��������
        {
            data_union_st->trace_st.settle_plat_amt = data_union_st->trace_st.trans_amt - data_union_st->trace_st.mdr_host_amt ;
        }
        else if ( atoi(data_union_st->shop_clear_mode) == SHOP_STL_MODE_FUND ) //��������
        {
            data_union_st->trace_st.settle_plat_amt = data_union_st->trace_st.mdr_plat_amt;
        }
    }
    
    //72. POS���㹴�Ա�־��������     0��ƽ�� 1:�ն˶� 2:ƽ̨�� 9�쳣
    {
        //  if (app_data->system_st.dup_flag == 1)
        //      data_union_st->trace_st.settle_pos_flag [0] = '1';
        //  else
            data_union_st->trace_st.settle_pos_flag [0] = '0';
    }

    //73. ͨ������    0��ƽ�� 1��׷�� 2������ 3������ 9���쳣
    {
        data_union_st->trace_st.settle_host_flag[0] = '0';
    }

    //74 75 ͨ��Ӧ��������
    {
        // settle_plat_amtsettle_plat_amt ; //    ͨ��Ӧ��������    ƽ̨
        data_union_st->trace_st.settle_host_amt = data_union_st->trace_st.settle_plat_amt; //    ͨ��ʵ��������    ͨ��/������ʼֵͬ
    }

    //76 77 ͨ�����㷽ʽ    0���յ�ģʽ�����壩 1������ģʽ
    {
        data_union_st->trace_st.host_clear_type[0] = '1';

        //data_union_st->trace_st.host_clear_date    = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.host_clear_date    = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //78 79 �̻����㷽ʽ    0���̻�ֱ��1ͨ��ֱ�ӵ��̻� 2�������̻� 3ƽ̨���̻� 4ƽ̨�������̻� 5ƽ̨������
    {
        data_union_st->trace_st.shop_clear_type[0] = data_union_st->shop_clear_mode[0];

        //data_union_st->trace_st.shop_clear_date      = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.shop_clear_date      = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //80 81 ��֧������������ ���������������
    {
        //data_union_st->trace_st.branch_clear_date    = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.branch_clear_date    = atoi(data_union_st->file_acoma_st.f_file_date);
        //data_union_st->trace_st.partner_clear_date   = atoi(data_union_st->file_acoma_st.f_clear_date);
        data_union_st->trace_st.partner_clear_date   = atoi(data_union_st->file_acoma_st.f_file_date);
    }

    //82. ����־    0������ 1�ܸ� 2���� ��������̱�
    {
        data_union_st->trace_st.error_point[0]  = '0';
    }

    //83. ���յȼ�    0���������������������չ���
    {
        data_union_st->trace_st.risk_flag[0] = '0';
    }

    //84. MACѺ��
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
 *���˿��ƺ���
 **/
int insert_exec(MYSQL *conn_ptr)
{
    DATA_UNION_ST   data_union_st;
    DB_RESULT       *db_result = NULL;
    int             ret, i ;
    char            sql[1024]     = { 0 };
    char            sql_info[512] = { 0 };

    db_result = (DB_RESULT *) malloc (sizeof(DB_RESULT));

    /*׼���α�*/
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

    /*ִ���α�*/
    db_exec_cursor(conn_ptr, db_result, sql, sql_info, LOG_SQL_TITLE);
    SysLog( LOGTYPE_INFO , "��Ҫ�������ˮ����[%d]", db_result->num_rows );
    for( i = 0; i < db_result->num_rows; i++ )
    {
        memset( &data_union_st, 0x00, sizeof( DATA_UNION_ST ) );
        /*��ȡ��ˮ����*/
        ret = db_get_file_acoma_info(db_result, &data_union_st, sql_info);
        if (ret == -2)
        {
            SysLog( LOGTYPE_INFO , "��������" );
            break;
        }
        
        /*ϵͳ��ˮ�ظ�У��*/
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
            //�ѵǼǹ������ٵǼ�
            continue;
        }
        
        /*ͨ��zpsum�ļ�����������˺��յ�����*/
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
            //ֱ��������������
            sprintf( data_union_st.shop_clear_mode, "%d", SHOP_STL_MODE_FUND ); //��������
        }
        else
        {
            //ֱ���յ���������
            sprintf( data_union_st.shop_clear_mode, "%d", SHOP_STL_MODE_ACQ ); //��������
        }
        
        /*��ȡ��bin����*/
        ret = db_get_cardbin_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            SysLog( LOGTYPE_ERROR , "[%s]δƥ�䵽��bin", data_union_st.file_acoma_st.f_card_no );
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        ret = db_get_trantype_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            SysLog( LOGTYPE_ERROR , "[%s][%s][%s]δƥ�䵽��������"
                   , data_union_st.file_acoma_st.f_msg_code
                   , data_union_st.file_acoma_st.f_tran_code
                   , data_union_st.file_acoma_st.f_sub_tran_code
            );
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*��ȡ��bin����*/
        ret = db_get_file_lfee_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            SysLog( LOGTYPE_ERROR , "�˽���[%s-%s]��Ʒ�Ʒ����", data_union_st.file_acoma_st.f_tran_time, data_union_st.file_acoma_st.f_trace_no );
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*��ȡ������Ϣ*/
        ret = db_get_city_info(conn_ptr, &data_union_st);
        if (ret < 0 && ret != SQLNOFOUND )
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*�����ͳ�����ȡԭ��������*/
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
        
        /*��ȡ��ˮ����*/
        ret = db_get_sys_info(conn_ptr, &data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*׼����ˮ����*/
        ret = func_prepare_trace_st(&data_union_st);
        if (ret < 0)
        {
            db_rollback (conn_ptr);
            db_free_result(db_result);
            return (ret);
        }
        
        /*�Ǽ�ϵͳ��ˮ*/
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
 *���˽���
 **/
int insert_init(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "�Ǽ���ˮ��ʼ����" );
    
    get_dayend_date( szDailyDate );
    strcpy( db_posp_name, getenv("MYSQL_POSP_NAME") );
    
    return NOERR;
}

/**
 *���˽���
 **/
int insert_end(MYSQL *conn_ptr)
{
    SysLog( LOGTYPE_INFO , "�Ǽ���ˮ��������" );
    
    return NOERR;
}

/**
 *��ں���
 **/
int func_insert_trace(MYSQL *conn_ptr)
{
    int    iRet          = -1;
    
    /*�Ǽ���ˮ��ʼ��*/
    iRet = insert_init(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "�Ǽ���ˮ��ʼ��ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*�Ǽ���ˮ*/
    iRet = insert_exec(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "�Ǽ���ˮʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*�Ǽ���ˮ����*/
    iRet = insert_end(conn_ptr);
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "�Ǽ���ˮ����ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    return STEP_EXEC_STATUS_SUCC;
}