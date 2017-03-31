/****************************************************************************
 *    Copyright (c) :裕福集团-软件公司.                      *
 *    ProgramName : main
 *    SystemName  : 裕福支付数据分析系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : Linux Server release 6.3
                    Oracle  11g
 *    Description : 数据分析系统-公共常量头文件
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/23         北京           李君凯         创建文档
****************************************************************************/

#define  SQLCODE sqlca.sqlcode
#define  SQLTEXT sqlca.sqlerrm.sqlerrmc
#define SQLNOTFOUND  100
#define SQL_OK  0

/*程序退出标识*/
#define     _EXIT_SUCCESS      0      /*程序正常退出*/
#define     _EXIT_FAILURE      -1     /*程序异常退出*/

/*函数返回标识*/
#define     NOERR      0              /*函数返回成功*/
#define     ERROR      -1             /*函数返回有错误*/

/*系统运行状态*/
#define   SYS_STATUS_NOM    1      /*系统正常*/
#define   SYS_STATUS_END    2      /*系统日终*/

/*步骤执行状态*/
const int STEP_EXEC_STATUS_INIT = 0;     /*未执行*/
const int STEP_EXEC_STATUS_SUCC = 1;     /*成功*/
const int STEP_EXEC_STATUS_FAIL = 2;     /*失败*/
const int STEP_EXEC_STATUS_DOING = 3;    /*正在执行*/
const int STEP_EXEC_STATUS_UNKNOWN = 4;  /*未知*/

/*所文件规则-避免重复*/
#define   LOCK_FILE_PREFIX_STEP          "step"    /*前缀-日终步骤*/
#define   LOCK_FILE_DAYENDMAIN           "main"      /*前缀-收单机构对账*/
#define   LOCK_FILE_DAYENDSTART           "start"      /*前缀-收单机构对账*/