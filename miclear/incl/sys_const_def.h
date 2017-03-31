/****************************************************************************
 *    Copyright (c) :ԣ������-�����˾.                      *
 *    ProgramName : main
 *    SystemName  : ԣ��֧�����ݷ���ϵͳ
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : Linux Server release 6.3
                    Oracle  11g
 *    Description : ���ݷ���ϵͳ-��������ͷ�ļ�
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/23         ����           �����         �����ĵ�
****************************************************************************/

#define  SQLCODE sqlca.sqlcode
#define  SQLTEXT sqlca.sqlerrm.sqlerrmc
#define SQLNOTFOUND  100
#define SQL_OK  0

/*�����˳���ʶ*/
#define     _EXIT_SUCCESS      0      /*���������˳�*/
#define     _EXIT_FAILURE      -1     /*�����쳣�˳�*/

/*�������ر�ʶ*/
#define     NOERR      0              /*�������سɹ�*/
#define     ERROR      -1             /*���������д���*/

/*ϵͳ����״̬*/
#define   SYS_STATUS_NOM    1      /*ϵͳ����*/
#define   SYS_STATUS_END    2      /*ϵͳ����*/

/*����ִ��״̬*/
const int STEP_EXEC_STATUS_INIT = 0;     /*δִ��*/
const int STEP_EXEC_STATUS_SUCC = 1;     /*�ɹ�*/
const int STEP_EXEC_STATUS_FAIL = 2;     /*ʧ��*/
const int STEP_EXEC_STATUS_DOING = 3;    /*����ִ��*/
const int STEP_EXEC_STATUS_UNKNOWN = 4;  /*δ֪*/

/*���ļ�����-�����ظ�*/
#define   LOCK_FILE_PREFIX_STEP          "step"    /*ǰ׺-���ղ���*/
#define   LOCK_FILE_DAYENDMAIN           "main"      /*ǰ׺-�յ���������*/
#define   LOCK_FILE_DAYENDSTART           "start"      /*ǰ׺-�յ���������*/