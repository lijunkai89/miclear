/****************************************************************************
 *    Copyright (c) :裕福集团-软件公司.                      *
 *    ProgramName : main
 *    SystemName  : 裕福支付数据分析系统
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : Linux Server release 6.3
                    Oracle  11g
 *    Description : 数据分析系统-日志头文件
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/23         北京           李君凯         创建文档
****************************************************************************/

/*日志文件前缀*/
#define		LOG_NAME				"dayend"
#define		LOG_NAME_START				"dayendstart"

#if 0
/*       日志类型定义       */
#define LOGTYPE_DEBUG     0x10       
#define LOGTYPE_INFO      0x20       
#define LOGTYPE_WARNING   0x40       
#define LOGTYPE_ERROR     0x80
#endif
/*       日志类型定义       */
#define LOGTYPE_DEBUG     1       
#define LOGTYPE_INFO      2       
#define LOGTYPE_WARNING   3       
#define LOGTYPE_ERROR     4

void vlog( unsigned char ucLogType , 
           char* szPrefix , 
           const char* szFile , 
           int nLine ,
           const char* szFormat , 
           va_list ap );