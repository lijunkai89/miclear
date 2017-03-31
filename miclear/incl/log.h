/****************************************************************************
 *    Copyright (c) :ԣ������-�����˾.                      *
 *    ProgramName : main
 *    SystemName  : ԣ��֧�����ݷ���ϵͳ
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : Linux Server release 6.3
                    Oracle  11g
 *    Description : ���ݷ���ϵͳ-��־ͷ�ļ�
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2014/12/23         ����           �����         �����ĵ�
****************************************************************************/

/*��־�ļ�ǰ׺*/
#define		LOG_NAME				"dayend"
#define		LOG_NAME_START				"dayendstart"

#if 0
/*       ��־���Ͷ���       */
#define LOGTYPE_DEBUG     0x10       
#define LOGTYPE_INFO      0x20       
#define LOGTYPE_WARNING   0x40       
#define LOGTYPE_ERROR     0x80
#endif
/*       ��־���Ͷ���       */
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