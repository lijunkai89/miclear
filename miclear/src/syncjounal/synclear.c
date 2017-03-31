/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������ϵͳ
 *    SystemName  : ���Ȳ���-ץȡ�������
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-ץȡ�������
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/26         ����           �����         �����ĵ�
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

static char  szDailyDate[9]    = { 0 };
#if 0
/**
 *���ܴ˲�
 *ɾ��������ͬ������ˮ����������������
 **/
int sync_jounal_init()
{
    int iRet = 0;
    char  szSql[2048]  = { 0 };

    /*ɾ��������ϸ*/
    sprintf( szSql , 
            "DELETE FROM T_TRAN_JOUNAL_SPLIT_DTL T "
            " WHERE EXISTS (SELECT 1 "
            "          FROM T_TRAN_JOUNAL T1 "
            "         WHERE T.KEY_RSP = T1.KEY_RSP "
            "           AND T1.SPLIT_DATE = \'%s\') "            
            , szDailyDate
    );
    
    iRet = EXEC_SQL_LOG(  szSql , "ɾ��������ϸ"  ) ;
    if ( iRet && SQLNOTFOUND != iRet )
    {
        DBROLLBACK();
        return ERROR ;
    }
    
    /*ɾ����ˮ*/
    memset( szSql , 0x00 , sizeof( szSql ) );
    sprintf( szSql , 
            " DELETE FROM T_TRAN_JOUNAL T "
            "  WHERE T.HOST_LOGIC_DATE = \'%s\' "
            , szDailyDate
    );
    
    iRet = EXEC_SQL_LOG(  szSql , "��ʼ��������ˮ��"  ) ;
    if ( iRet && SQLNOTFOUND != iRet )
    {
        DBROLLBACK();
        return ERROR ;
    }
    
    
    /*����ʷ��ˮ�̳����ղ��˵���ʷ��ˮ*/
    memset( szSql , 0x00 , sizeof( szSql ) );
    sprintf( szSql , 
            "INSERT INTO T_TRAN_JOUNAL "
            "  (KEY_RSP, "
            "   CARD_NO, "
            "   DST_SRVID, "
            "   TRAN_TYPE, "
            "   VOID_TRAN_TYPE, "
            "   MSG_TYPE, "
            "   PROC_CODE, "
            "   LOCAL_SYS_DATE, "
            "   LOCAL_SYS_TIME, "
            "   TRAN_SYS_TIME, "
            "   HOST_TRAN_TIME, "
            "   TRAN_AMT, "
            "   VOID_AMT, "
            "   TERMINAL_ID, "
            "   MERCHANT_ID, "
            "   OTHER_TERMINAL_ID, "
            "   OTHER_MERCHANT_ID, "
            "   MERCHANT_NAME, "
            "   MCC, "
            "   TRACE_NO, "
            "   VOID_TRACE_NO, "
            "   TRAN_RRN, "
            "   VOID_RRN, "
            "   BANK_RRN, "
            "   HOST_LS_NO, "
            "   BANK_BATCH_NO, "
            "   CARD_TYPE, "
            "   ACQ_BANK_ID, "
            "   ISS_BANK_ID, "
            "   SND_BANK_ID, "
            "   RCV_BANK_ID, "
            "   AUTH_NO, "
            "   RESP_CODE, "
            "   HOST_LOGIC_DATE, "
            "   TRAN_FLAG, "
            "   SAF_FLAG, "
            "   ORG_TRAN_TIME, "
            "   CHECK_FLAG) "
            "  SELECT V.KEY_RSP, "
                    " V.CARD_NO, "
                    " V.DST_SRVID, "
                    " V.TRAN_TYPE, "
                    " V.VOID_TRAN_TYPE, "
                    " V.MSG_TYPE, "
                    " V.PROC_CODE, "
                    " V.LOCAL_SYS_DATE, "
                    " V.LOCAL_SYS_TIME, "
                    " V.TRAN_SYS_TIME, "
                    " V.HOST_TRAN_TIME, "
                    " V.TRAN_AMT, "
                    " V.VOID_AMT, "
                    " V.TERMINAL_ID, "
                    " V.MERCHANT_ID, "
                    " V.OTHER_TERMINAL_ID, "
                    " V.OTHER_MERCHANT_ID, "
                    " V.MERCHANT_NAME, "
                    " V.MCC, "
                    " V.TRACE_NO, "
                    " V.VOID_TRACE_NO, "
                    " V.TRAN_RRN, "
                    " V.VOID_RRN, "
                    " V.BANK_RRN, "
                    " V.HOST_LS_NO, "
                    " V.BANK_BATCH_NO, "
                    " V.CARD_TYPE, "
                    " V.ACQ_BANK_ID, "
                    " V.ISS_BANK_ID, "
                    " V.SND_BANK_ID, "
                    " V.RCV_BANK_ID, "
                    " V.AUTH_NO, "
                    " V.RESP_CODE, "
                    " V.HOST_LOGIC_DATE, "
                    " V.TRAN_FLAG, "
                    " V.SAF_FLAG, "
                    " V.ORG_TRAN_TIME, "
                    " CASE "
                    "   WHEN (V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d ) "
                    "       AND (V.TRAN_FLAG = %d OR V.TRAN_FLAG = %d) "
                    "       AND V.SAF_FLAG = %d "
                    "       AND V.RESP_CODE = '00' "
                    "   THEN "
                    "       %d "
                    "   ELSE "
                    "       %d "
                    " END "
            "    FROM T_TRAN_JOUNAL_HIS V "
            "   WHERE V.CHECK_DATE = \'%s\' "
            "     AND V.CHECK_DATE > V.HOST_LOGIC_DATE "
            "     AND NOT EXISTS( SELECT 1 "
            "                       FROM T_TRAN_JOUNAL T1 "
            "                      WHERE V.KEY_RSP = T1.KEY_RSP )"
            , TRAN_TYPE_PAY , TRAN_TYPE_PAY_REVA
            , TRAN_TYPE_ACK , TRAN_TYPE_ACK_REVA
            , TRAN_TYPE_REF
            , TRAN_FLAG_FINISH , TRAN_FLAG_RAVA
            , TRAN_SAF_FLAG_NOT
            , FUNDCHNL_CHK_STATUS_INIT
            , FUNDCHNL_CHK_STATUS_NONEED
            , szDailyDate
    );
    
    iRet = EXEC_SQL_LOG(  szSql , "����ʷ��ˮ�̳����ղ��˵���ʷ��ˮ"  ) ;
    if ( iRet && SQLNOTFOUND != iRet )
    {
        DBROLLBACK();
        return ERROR ;
    }
    
    /*ɾ����ʷ��ˮ���ղ��˵���ʷ��ˮ*/
    memset( szSql , 0x00 , sizeof( szSql ) );
    sprintf( szSql , 
            " DELETE FROM T_TRAN_JOUNAL_HIS T "
            "  WHERE T.CHECK_DATE = \'%s\' "
            "    AND T.CHECK_DATE > T.HOST_LOGIC_DATE "
            , szDailyDate
    );
    
    iRet = EXEC_SQL_LOG(  szSql , "ɾ����ʷ��ˮ���ղ��˵���ʷ��ˮ"  ) ;
    if ( iRet && SQLNOTFOUND != iRet )
    {
        DBROLLBACK();
        return ERROR ;
    }
    
    DBCOMMIT();
    return NOERR ;
}

/**
 *��pospϵͳ��ȡ������ˮ���������������׺ͳ���ԭ���ף�
 *�������ͣ����ѡ�Ԥ��Ȩ��ɡ��˻�������
 *����״̬��1-���سɹ� 2-������ɻ��ѳ���
 **/
int sync_jounal_do()
{
    int iRet = 0;
    char  szSql[3072]  = { 0 };
    
    sprintf( szSql , 
            "INSERT INTO T_TRAN_JOUNAL "
            "  (KEY_RSP, "
            "   CARD_NO, "
            "   DST_SRVID, "
            "   TRAN_TYPE, "
            "   VOID_TRAN_TYPE, "
            "   MSG_TYPE, "
            "   PROC_CODE, "
            "   LOCAL_SYS_DATE, "
            "   LOCAL_SYS_TIME, "
            "   TRAN_SYS_TIME, "
            "   HOST_TRAN_TIME, "
            "   TRAN_AMT, "
            "   VOID_AMT, "
            "   TERMINAL_ID, "
            "   MERCHANT_ID, "
            "   OTHER_TERMINAL_ID, "
            "   OTHER_MERCHANT_ID, "
            "   MERCHANT_NAME, "
            "   MCC, "
            "   TRACE_NO, "
            "   VOID_TRACE_NO, "
            "   TRAN_RRN, "
            "   VOID_RRN, "
            "   BANK_RRN, "
            "   HOST_LS_NO, "
            "   BANK_BATCH_NO, "
            "   CARD_TYPE, "
            "   ACQ_BANK_ID, "
            "   ISS_BANK_ID, "
            "   SND_BANK_ID, "
            "   RCV_BANK_ID, "
            "   AUTH_NO, "
            "   RESP_CODE, "
            "   HOST_LOGIC_DATE, "
            "   TRAN_FLAG, "
            "   SAF_FLAG, "
            "   ORG_TRAN_TIME, "
            "   CHECK_FLAG) "
            "  SELECT TRIM(V.KEY_RSP), "
            "         V.CARD_NO, "
            "         V.DST_SRVID, "
            "         V.TRAN_TYPE, "
            "         V.VOID_TRAN_TYPE, "
            "         V.MSG_TYPE, "
            "         V.PROC_CODE, "
            "         V.LOCAL_SYS_DATE, "
            "         V.LOCAL_SYS_TIME, "
            "         V.TRAN_SYS_TIME, "
            "         TRIM(V.HOST_TRAN_TIME), "
            "         DECODE(T.IO_FLAG, \'%s\', -1 * V.TRAN_AMT, V.TRAN_AMT), "
            "         DECODE(T.IO_FLAG, \'%s\', V.TRAN_AMT, -1 * V.VOID_AMT), "
            "         V.TERMINAL_ID, "
            "         V.MERCHANT_ID, "
            "         V.OTHER_TERMINAL_ID, "
            "         V.OTHER_MERCHANT_ID, "
            "         V.MERCHANT_NAME, "
            "         V.MCC, "
            "         V.TRACE_NO, "
            "         V.VOID_TRACE_NO, "
            "         V.TRAN_RRN, "
            "         V.VOID_RRN, "
            "         V.BANK_RRN, "
            "         V.HOST_LS_NO, "
            "         V.BANK_BATCH_NO, "
            "         V.CARD_TYPE, "
            "         V.ACQ_BANK_ID, "
            "         V.ISS_BANK_ID, "
            "         V.SND_BANK_ID, "
            "         V.RCV_BANK_ID, "
            "         V.AUTH_NO, "
            "         V.RESP_CODE, "
            "         V.HOST_LOGIC_DATE, "
            "         V.TRAN_FLAG, "
            "         V.SAF_FLAG, "
            "         V.ORG_TRAN_TIME, "
            "         CASE "
            "           WHEN (V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d OR V.TRAN_TYPE = %d ) "
            "               AND (V.TRAN_FLAG = %d OR V.TRAN_FLAG = %d) "
            "               AND V.SAF_FLAG = %d "
            "               AND V.RESP_CODE = '00' "
            "           THEN "
            "               %d "
            "           ELSE "
            "               %d "
            "         END "
            "    FROM VIEW_POSP_TRAN_JOURNAL V , T_TRAN_TYPE T "
            "   WHERE V.HOST_LOGIC_DATE = \'%s\' "
            "     AND V.TRAN_TYPE = T.SYS_TRAN_CODE(+) "
            , TRAN_TYPE_IO_FLAG_O , TRAN_TYPE_IO_FLAG_O
            , TRAN_TYPE_PAY , TRAN_TYPE_PAY_REVA
            , TRAN_TYPE_ACK , TRAN_TYPE_ACK_REVA
            , TRAN_TYPE_REF
            , TRAN_FLAG_FINISH , TRAN_FLAG_RAVA
            , TRAN_SAF_FLAG_NOT
            , FUNDCHNL_CHK_STATUS_INIT
            , FUNDCHNL_CHK_STATUS_NONEED
            , szDailyDate
    );
    
    SysLog( LOGTYPE_DEBUG , "szSql=[%S]" , szSql );
    iRet = EXEC_SQL_LOG(  szSql , "ͬ����ˮ"  ) ;
    if ( iRet && SQLNOTFOUND != iRet )
    {
        DBROLLBACK();
        return ERROR ;
    }
    
    DBCOMMIT();
    return NOERR ;
}
#endif
/**
 *��ں���
 **/
int func_syno_clear(MYSQL *conn_ptr)
{
    int iRet = -1;

    SysLog( LOGTYPE_INFO , "ͬ����ˮ��ʼ����" );
    
    get_dayend_date( szDailyDate );
    SysLog( LOGTYPE_INFO , "szDailyDate = [%s]", szDailyDate );
    #if 0
    /*��ʼ��*/
    iRet = sync_jounal_init();
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "ͬ����ˮ��ʼ��ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*ͬ����ˮ*/
    iRet = sync_jounal_do();
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "ͬ����ˮʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }
    #endif
    SysLog( LOGTYPE_INFO , "ͬ����ˮ��������" );

    return STEP_EXEC_STATUS_SUCC;
}
