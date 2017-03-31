/****************************************************************************
 *    Copyright (c) :С��-С��֧��.                      *
 *    ProgramName : �������ϵͳ
 *    SystemName  : ���Ȳ���-װ�ش��������ļ�
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : �������ϵͳ-װ�ش��������ļ�
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/09         ����           �����         �����ĵ�
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

#define  VALUE_SEP_CHAR  ','
#define LOG_SQL_TITLE "װ�������ļ�"
/*��������*/
#define  VALUE_DEF_TYPE_INT     'i'
#define  VALUE_DEF_TYPE_FLOAT     'f'
#define  VALUE_DEF_TYPE_STRING  'c'

#define  load_sql_sh  "load_mysql_sql_file.sh"

static char  szDailyDate[9]    = { 0 };

/*ACOMN�ļ���Ҫ����ֶ��б�*/
typedef struct 
{
    int  valueth;  /*���ļ��е��ֶ����*/
    int  valuelen;  /*�򳤶�*/
    int  getflag;  /*�Ƿ���Ҫȡ�� 0-����Ҫ 1-��Ҫ*/
    char type;     /*�Ƿ���Ҫȡ�� 'c'-�ַ��� 'i'-��ֵ*/
}record_st;

record_st ACOMN[] = {
  { 1  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���������ʶ��*/
  { 2  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���ͻ�����ʶ��*/
  { 3  , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*ϵͳ���ٺ�*/
  { 4  , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���״���ʱ��*/
  { 5  , 19 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���ź�*/
  { 6  , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*���׽��*/
  { 7  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*���ִ���ʱ�ĳжҽ��*/
  { 8  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*�ֿ��˽���������*/
  { 9  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��������*/
  { 10 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*����������*/
  { 11 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�����*/
  { 12 , 8  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�ն˺�*/
  { 13 , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻���*/
  { 14 , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�����ο���*/
  { 15 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�����������*/
  { 16 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��ȨӦ����*/
  { 17 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���ջ�����ʶ��*/
  { 18 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*ԭʼ����ϵͳ���ٺ�*/
  { 19 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*���׷�����*/
  { 20 , 3  , 0 , VALUE_DEF_TYPE_STRING } ,  /*��������뷽ʽ*/
  { 21 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*����Ӧ��������*/
  { 22 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*����Ӧ��������*/
  { 23 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*ת�ӷ����*/
  { 24 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*��˫ת����ʶ*/
  { 25 , 3  , 0 , VALUE_DEF_TYPE_STRING } ,  /*��Ƭ���к�*/
  { 26 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�ն˶�ȡ����*/
  { 27 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*IC��������*/
  { 28 , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*ԭʼ��������ʱ��*/
  { 29 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*����������ʶ��*/
  { 30 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*���׵�����ʶ*/
  { 31 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�ն�����*/
  { 32 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*ECI��ʶ*/
  { 33 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*���ڸ����������*/
  { 34 , 14 , 0 , VALUE_DEF_TYPE_STRING } ,  /*������Ϣ*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/*ACOMA�ļ���Ҫ����ֶ��б�*/
record_st ACOMA[] = {
  { 1  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���������ʶ��*/
  { 2  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���ͻ�����ʶ��*/
  { 3  , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*ϵͳ���ٺ�*/
  { 4  , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���״���ʱ��*/
  { 5  , 19 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���ź�*/
  { 6  , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*���׽��*/
  { 7  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*���ִ���ʱ�ĳжҽ��*/
  { 8  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*�ֿ��˽���������*/
  { 9  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��������*/
  { 10 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*����������*/
  { 11 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�����*/
  { 12 , 8  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�ն˺�*/
  { 13 , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻���*/
  { 14 , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�����ο���*/
  { 15 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�����������*/
  { 16 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��ȨӦ����*/
  { 17 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���ջ�����ʶ��*/
  { 18 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*ԭʼ����ϵͳ���ٺ�*/
  { 19 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*���׷�����*/
  { 20 , 3  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��������뷽ʽ*/
  { 21 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*����Ӧ��������*/
  { 22 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*����Ӧ��������*/
  { 23 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*ת�ӷ����*/
  { 24 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*��˫ת����ʶ*/
  { 25 , 3  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��Ƭ���к�*/
  { 26 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�ն˶�ȡ����*/
  { 27 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*IC��������*/
  { 28 , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*ԭʼ��������ʱ��*/
  { 29 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*����������ʶ��*/
  { 30 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*���׵�����ʶ*/
  { 31 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�ն�����*/
  { 32 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*ECI��ʶ*/
  { 33 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*���ڸ����������*/
  { 34 , 14 , 0 , VALUE_DEF_TYPE_STRING } ,  /*������Ϣ*/
  { 35 , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*���ͷ��������*/
  { 36 , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*���շ��������*/
  { 37 , 1  , 1 , VALUE_DEF_TYPE_STRING } ,  /*������ʶ*/
  { 38 , 1  , 1 , VALUE_DEF_TYPE_STRING } ,  /*������ʶ*/
  { 39 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��������*/
  { 40 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*���㳡��*/
  { 41 , 40 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻����Ƶ�ַ*/
  { 42 , 3  , 0 , VALUE_DEF_TYPE_STRING } ,  /*���ױ���*/
  { 43 , 9  , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*�������������յ������Զ��ۿ�������*/
  { 44 , 9  , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*�̻�������*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/*LFE�ļ���Ҫ����ֶ��б�*/
record_st LFEE[] = {
  { 1  , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*����һ�����д���*/
  { 2  , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�Ƿ�������׼��*/
  { 3  , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*����ǿ���ʶ*/
  { 4  , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�ն�����*/
  { 5  , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*������*/
  { 6  , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*���׵����ʶ*/
  { 7  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�����*/
  { 8  , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*�����������д���*/
  { 9  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�����������*/
  { 10 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*ת����������*/
  { 11 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*������������*/
  { 12 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���ջ�������*/
  { 13 , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*�յ���������*/
  { 14 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*��������*/
  { 15 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*���״�����*/
  { 16 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�����������*/
  { 17 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*ϵͳ���ٺ�*/
  { 18 , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���״�������ʱ��*/
  { 19 , 19 , 1 , VALUE_DEF_TYPE_STRING } ,  /*���˺�*/
  { 20 , 28 , 0 , VALUE_DEF_TYPE_STRING } ,  /*ת����*/
  { 21 , 28 , 0 , VALUE_DEF_TYPE_STRING } ,  /*ת�뿨*/
  { 22 , 42 , 1 , VALUE_DEF_TYPE_STRING } ,  /*ԭʼ������Ϣ*/
  { 23 , 8  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�ܿ����ն˱�ʶ��*/
  { 24 , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�ܿ�����ʶ��*/
  { 25 , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�����ο���*/
  { 26 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*���׽��*/
  { 27 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*����������*/
  { 28 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*Ʒ�Ʒ����*/
  { 29 , 12 , 0 , VALUE_DEF_TYPE_STRING } ,  /*����ʹ��*/
  { 30 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*�����*/
  { 31 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*���׷���ʽ*/
  { 32 , 99 , 0 , VALUE_DEF_TYPE_STRING } ,  /*����ʹ��*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/*ERRA�ļ���Ҫ����ֶ��б�*/
record_st ERRA[] = {
  { 1  , 3  , 1 , VALUE_DEF_TYPE_STRING } , /*�������*/
  { 2  , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*���������ʶ��*/
  { 3  , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*���ͻ�����ʶ��*/
  { 4  , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*ϵͳ���ٺ�*/
  { 5  , 10 , 1 , VALUE_DEF_TYPE_STRING } , /*���״���ʱ��*/
  { 6  , 19 , 1 , VALUE_DEF_TYPE_STRING } , /*���˺�*/
  { 7  , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*���׽��*/
  { 8  , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*��������*/
  { 9  , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*����������*/
  { 10 , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*�̻�����*/
  { 11 , 8  , 1 , VALUE_DEF_TYPE_STRING } , /*�ܿ����ն˱�ʶ��*/
  { 12 , 12 , 1 , VALUE_DEF_TYPE_STRING } , /*��һ�ʽ��׼����ο���*/
  { 13 , 2  , 1 , VALUE_DEF_TYPE_STRING } , /*�����������*/
  { 14 , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*��ȨӦ����*/
  { 15 , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*���ջ�����ʶ��*/
  { 16 , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*�������б�ʶ��*/
  { 17 , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*��һ�ʽ���ϵͳ���ٺ�*/
  { 18 , 2  , 1 , VALUE_DEF_TYPE_STRING } , /*���׷�����*/
  { 19 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*��������뷽ʽ*/
  { 20 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*����Ӧ��������*/
  { 21 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*����Ӧ��������*/
  { 22 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*���ڸ����������*/
  { 23 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*�ֿ��˽���������*/
  { 24 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*Ӧ�շ���*/
  { 25 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*Ӧ������*/
  { 26 , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*���ԭ��*/
  { 27 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*ת��������ʶ��*/
  { 28 , 19 , 0 , VALUE_DEF_TYPE_STRING } , /*ת���˺�*/
  { 29 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*ת�������ʶ��*/
  { 30 , 19 , 0 , VALUE_DEF_TYPE_STRING } , /*ת���˺�*/
  { 31 , 10 , 1 , VALUE_DEF_TYPE_STRING } , /*��һ�ʵ�����ʱ��*/
  { 32 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*��Ƭ���к�*/
  { 33 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*�ն˶�ȡ����*/
  { 34 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*IC����������*/
  { 35 , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*��һ�ʽ�����������*/
  { 36 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*��һ�ʽ��׽��*/
  { 37 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*���׵����ʶ*/
  { 38 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*ECI��ʶ*/
  { 39 , 15 , 1 , VALUE_DEF_TYPE_STRING } , /*�̻�����*/
  { 40 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*���ͷ��������*/
  { 41 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*ת�����������*/
  { 42 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*ת�뷽�������*/
  { 43 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*��һ�ʽ����ն�����*/
  { 44 , 40 , 1 , VALUE_DEF_TYPE_STRING } , /*�̻����Ƶ�ַ*/
  { 45 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*����Ʒ�����*/
  { 46 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*����Ʒѵ���*/
  { 47 , 8  , 0 , VALUE_DEF_TYPE_STRING } , /*����ʹ��*/
  { 48 , 24 , 0 , VALUE_DEF_TYPE_STRING } , /*����Ʒ��ʶ��Ϣ*/
  { 49 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*��������׵���ԭʼ�Ľ��׵Ľ��״���*/
  { 50 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*���׷���*/
  { 51 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*�˻���������*/
  { 52 , 46 , 0 , VALUE_DEF_TYPE_STRING } , /*����ʹ��*/
  { 53 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*ԭʼ��������*/
  { 54 , 9  , 1 , VALUE_DEF_TYPE_FLOAT } , /*�̻�������*/
  { 55 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*�̻�������*/
  { 56 , 9  , 0 , VALUE_DEF_TYPE_FLOAT } , /*�̻������з���*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

record_st ZSUM[] = {
  { 1  , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�����*/
  { 2  , 80 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�ȫ��*/
  { 3  , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*���׽��*/
  { 4  , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*�����ѽ��*/
  { 5  , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*������*/
  { 6  , 200 , 0 , VALUE_DEF_TYPE_STRING } ,  /*����ʹ��*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

record_st ZPSUM[] = {
  { 1  , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�����*/
  { 2  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�����*/
  { 3  , 80 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�ȫ��*/
  { 4  , 60 , 1 , VALUE_DEF_TYPE_STRING } ,  /*����������*/
  { 5  , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*������֧��ϵͳ�к�*/
  { 6  , 12 , 0 , VALUE_DEF_TYPE_STRING } ,  /*�����������*/
  { 7  , 32 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻��˺�*/
  { 8  , 80 , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻��˻���*/
  { 9  , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*�̻�������������*/
  { 10 , 8  , 1 , VALUE_DEF_TYPE_INT } ,  /*�������ױ���*/
  { 11 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*�������׽��*/
  { 12 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*�������׷��ý��*/
  { 13 , 8  , 1 , VALUE_DEF_TYPE_INT } ,  /*����ױ���*/
  { 14 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*����׽��*/
  { 15 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*����׻��˷��ý��*/
  { 16 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*�����ʽ��ܶ�*/
  { 17 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*������ܽ��*/
  { 18 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*��֧�����߷��õּ����*/
  { 19 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*��֧�����߷��õּ���*/
  { 20 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*���������ͷŽ��*/
  { 21 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*�����������˽��*/
  { 22 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*���ڻز����*/
  { 23 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*���˽��*/
  { 24 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�������η�*/
  { 25 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*�ز�����*/
  { 26 , 198 , 0 , VALUE_DEF_TYPE_STRING } ,  /*����ʹ��*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/**
 *�����Ȳ���ַ���
 *����˵����
 *      pread    �ַ���
 *      valuelen �ֶγ���
 *      buf       �ֶ�ֵ
 *      delin     �ָ���
 *����ֵ��
 *      ��ֺ���ַ���
 **/
char *_fun_cut_fileline( char *pread , char *buf , int valuelen , char delin )
{
    char *pret = NULL;
    
    if ( (pread + valuelen)[0] != delin )
    {
        /*��ĩβ�޷ָ���*/
        if ( strlen( pread ) < valuelen )
        {
            SysLog( LOGTYPE_ERROR , "�ָ�������[%c]" , (pread + valuelen)[0] );
        }
    }
    
    pret = pread + valuelen + 1;
    memset( buf , 0x00 , 50 );
    strncpy( buf , pread , valuelen );
    
    return pret;
}

/**
 *����ȡֵ
 *��Σ�value_in
 *      data_type
 *���ң�value_out
 *����ֵ��
 *˵�����տ�ף����н����ֵ��ʾ��+������Ľ��׽�
 *      ����ף����н�ֵ��ʾ������ȷӦ��Ӧ��������ֶ��⣩
 **/
int deal_data_type(char *value_in, char *value_out, char data_type)
{
    //�ַ����͵Ĵ���
   if ( data_type == VALUE_DEF_TYPE_FLOAT )
   {
       if ( value_in[0] == DC_FLAG_C )
       {
           sprintf( value_out , "%.2f" , atof(value_in + 1) );
       }
       else if ( value_in[0] == DC_FLAG_D )
       {
           sprintf( value_out , "%.2f" , -1 * atof(value_in + 1) );
       }
       else
       {
           sprintf( value_out , "%.2f" , atof(value_in) );
       }
   }
   else if ( data_type == VALUE_DEF_TYPE_INT )
   {
       sprintf( value_out , "%ld" , atol(value_in) );
   }
   else
   {
       sprintf( value_out, "%s", value_in );
   }
}

/**
 *�����ˮid
 *ȡ�����ŵĲ���
 *��Σ�value_in
 *      data_type
 *���ң�value_sql
 *����ֵ��
 *˵�����տ�ף����н����ֵ��ʾ��+������Ľ��׽�
 *      ����ף����н�ֵ��ʾ������ȷӦ��Ӧ��������ֶ��⣩
 **/
void deal_data_type_for_sql(char *value_in, char *value_sql, char data_type)
{
    //�ַ����͵Ĵ���
   if ( data_type == VALUE_DEF_TYPE_FLOAT )
   {
       if ( value_in[0] == DC_FLAG_D )
       {
           sprintf( value_sql , "%.2f" , atof(value_in + 1) );
       }
       else if ( value_in[0] == DC_FLAG_C )
       {
           sprintf( value_sql , "%.2f" , -1 * atof(value_in + 1) );
       }
       else
       {
           sprintf( value_sql , "%.2f" , atof(value_in) );
       }
   }
   else if ( data_type == VALUE_DEF_TYPE_INT )
   {
       sprintf( value_sql , "%ld" , atol(value_in) );
   }
   else
   {
       sprintf( value_sql, "\'%s\'", value_in );
   }
}

/**
 *������ļ���ȡ��������
 *���ش��������ݳ���
 **/
int deal_data_line( char *sLineBufR )
{
    if ( 0 == strlen(sLineBufR) )
    {
        return 0;
    }
    
    //��βȥ���з���\n��
    if ( 10 == sLineBufR[strlen(sLineBufR) - 1] )
    {
        sLineBufR[strlen(sLineBufR) - 1] = '\0';
    }
    //��βȥ�س���"\r"
    if ( 13 == sLineBufR[strlen(sLineBufR) - 1] )
    {
        sLineBufR[strlen(sLineBufR) - 1] = '\0';
    }
    
    return strlen( sLineBufR );
}

/*ִ��sql�ű�*/
int func_load_file( MYSQL *conn_ptr, char *sqlfile )
{
    FILE *fp_sql  = NULL;
    char  sLineBufR[4096]    = { 0 };     /*��ȡ�л���*/
    int   iRet               = -1;
        
    /*���ļ�*/
    if ( ( fp_sql = fopen( sqlfile , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "��sql�ű��ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp_sql))
    {
        //���ж�ȡ�ļ�
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        fgets( sLineBufR , sizeof(sLineBufR) , fp_sql ); 
        
        //�жϿ���
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        iRet = db_update( conn_ptr , sLineBufR, LOG_SQL_TITLE );
        if ( iRet <= 0 )
        {
            SAFE_FCLOSE( fp_sql );
            return ERROR;
        }
    }
    
    SAFE_FCLOSE( fp_sql );
    return NOERR;
}

/**
 *��ʽ���ļ���
 **/
int func_fmt_line( record_st *record_st, char *sLineBufR, char *sLineBufWT)
{
    char  *pread = NULL ;                     /*��ȡ��д����ָ��*/
    char  sFieldBuf[100]          =  { 0 };    /*��ֵ��ָ��*/
    char  sFieldBufT[100]          =  { 0 };    /*��ֵ��ָ��*/
    int  i  = 0;                /*������-�ֶθ���*/
    
    //�����л��д���
    deal_data_line(sLineBufR);

    pread = sLineBufR;
    //SysLog( LOGTYPE_DEBUG , "[%s]" , pread );

    for ( i = 0 ; record_st[i].valueth > 0 ; i++ )
    {
        memset( sFieldBuf , 0x00 , sizeof( sFieldBuf ) );
        memset( sFieldBufT , 0x00 , sizeof( sFieldBufT ) );
        pread = _fun_cut_fileline( pread , sFieldBufT , record_st[i].valuelen , FTP_FILE_SPLITCHAR );
        
        if ( record_st[i].getflag == 1 )
        {
            //�ַ����͵Ĵ���
            deal_data_type_for_sql(sFieldBufT, sFieldBuf, record_st[i].type);
            
            strcat( sLineBufWT , sFieldBuf );
            sLineBufWT[strlen(sLineBufWT)] = VALUE_SEP_CHAR ;
        }
    }
    
    return NOERR;
}

/**
 *����id�ҵ�ָ����ֵ
 **/
int get_value_by_id( record_st *record_st, char *sLineBufR, int id, char *value )
{
    char  *pread = NULL ;                     /*��ȡ��д����ָ��*/
    char  sFieldBuf[100]          =  { 0 };    /*��ֵ��ָ��*/
    char  sFieldBufT[100]          =  { 0 };    /*��ֵ��ָ��*/
    int  i  = 0;                /*������-�ֶθ���*/
    
    //��βȥ���з���\n��
    deal_data_line(sLineBufR);

    pread = sLineBufR;

    for ( i = 0 ; record_st[i].valueth > 0 ; i++ )
    {
        memset( sFieldBuf , 0x00 , sizeof( sFieldBuf ) );
        memset( sFieldBufT , 0x00 , sizeof( sFieldBufT ) );
        
        pread = _fun_cut_fileline( pread , sFieldBufT , record_st[i].valuelen , FTP_FILE_SPLITCHAR );
        
        if ( record_st[i].valueth == id )
        {
            deal_data_type(sFieldBufT, value, record_st[i].type);
            break;
        }
    }
    
    if ( record_st[i].valueth < 0 )
    {
        return ERROR;
    }
    
    return NOERR;
}

/**
 *װ�ض����ļ���acomn
 **/
int load_file_acomn( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*��ȡ�л���*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*д�л���*/
    char  sLineBufWT[4096]   = { 0 };    /*д�л���*/
    char  sPk_id[21]         = { 0 };    /*��ˮ�ļ���������*/
    FILE *fp  = NULL , *fp_sql = NULL;
    char  value[100]         = { 0 };
    int   channel_no     ;
    
    SysLog( LOGTYPE_DEBUG , "[%s]" , datafileallname );
    
    sprintf( sLineHeadW , 
            "insert into t_liquidate_file_dtl("
            "f_agent_code, f_snd_code, f_trace_no, "
            "f_tran_time, f_card_no, f_tran_amt, f_msg_code, "
            "f_tran_code, f_mcc, f_terminal_no, f_merchant_no, "
            "f_tran_rrn, f_sub_tran_code, f_auth_no, f_rcv_code, "
            "f_org_trace_no, f_resp_code, f_rcv_fee, f_pay_fee, "
            "f_tran_fee, f_org_tran_time, f_iss_code, "
            "f_data_acom, f_id, f_channel_no, f_check_flag, f_file_date) values"
    );
        
    /*���ļ�*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*���ļ�*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //���ж�ȡ�ļ�
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //�жϿ���
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*��ʽ�������ļ���*/
        iRet = func_fmt_line( ACOMN, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ʽ�������ļ���ʧ�ܣ�" );
            return ERROR;
        }
        
        /*ECI��ʶ----�������²��*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMN, sLineBufR, 32, value);
        SysLog( LOGTYPE_DEBUG , "[%s]" , value );
        if ( strcmp( value, " " ) == 0 )
        {
            /*����*/
            continue;
        }
        
        /*��ȡ��ˮid*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_dtl_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ȡ��ˮidʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ˮ�������봦��*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMN, sLineBufR, 10, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*��������*/
            channel_no = CHANNEL_CUP_DAIFU;
        }
        else if ( strcmp( value, "300000" ) == 0 )
        {
            continue;
        }
        else
        {
            /*ֱ������*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*ƴ��sql�ļ���*/
        sprintf( sLineBufW , "%s(%s\'%s\'%c\'%s\'%c%d%c\'%s\'%c\'%s\');\n" , 
                           sLineHeadW, sLineBufWT, 
                           sLineBufR, VALUE_SEP_CHAR, 
                           sPk_id, VALUE_SEP_CHAR, 
                           channel_no, VALUE_SEP_CHAR, 
                           FUNDCHNL_CHK_STATUS_INIT, VALUE_SEP_CHAR,
                           szDailyDate );
        //SysLog( LOGTYPE_DEBUG , "[%s]" , sLineBufW );
        fprintf( fp_sql , sLineBufW );
    }
    
    SAFE_FCLOSE( fp );
    SAFE_FCLOSE( fp_sql );
    
    return NOERR;
}

/**
 *װ�ض����ļ���acoma
 **/
int load_file_acoma( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*��ȡ�л���*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*д�л���*/
    char  sLineBufWT[4096]   = { 0 };    /*д�л���*/
    char  sPk_id[21]         = { 0 };    /*��ˮ�ļ���������*/
    FILE *fp  = NULL , *fp_sql = NULL;
    char  value[100]         = { 0 };
    int   channel_no     ;
    
    SysLog( LOGTYPE_DEBUG , "[%s]" , datafileallname );
    
    sprintf( sLineHeadW , 
            "insert into t_liquidate_file_acoma("
            "f_agent_code, f_snd_code, f_trace_no, "
            "f_tran_time, f_card_no, f_tran_amt, f_msg_code, "
            "f_tran_code, f_mcc, f_terminal_no, f_merchant_no, "
            "f_tran_rrn, f_sub_tran_code, f_auth_no, f_rcv_code, "
            "f_org_trace_no, f_resp_code, f_pos_mode, f_rcv_fee, f_pay_fee, "
            "f_tran_fee, f_card_sn, f_org_tran_time, f_iss_code, "
            "f_reversal_flag, f_cancle_flag, f_clear_date, "
            "f_merchant_name_add, f_merchant_fee, "
            "f_data_acoma, f_id, f_channel_no, f_file_date) values"
    );
        
    /*���ļ�*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*���ļ�*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //���ж�ȡ�ļ�
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //�жϿ���
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*��ʽ�������ļ���*/
        iRet = func_fmt_line( ACOMA, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ʽ�������ļ���ʧ�ܣ�" );
            return ERROR;
        }
        
        /*ECI��ʶ----�������²��*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMN, sLineBufR, 32, value);
        SysLog( LOGTYPE_DEBUG , "[%s]" , value );
        if ( strcmp( value, "  " ) != 0 )
        {
            /*����*/
            continue;
        }
        
        /*��ȡ��ˮid*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_acoma_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ȡ��ˮidʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ˮ�������봦��*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMA, sLineBufR, 10, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*��������*/
            channel_no = CHANNEL_CUP_DAIFU ;
        }
        else if ( strcmp( value, "300000" ) == 0 )
        {
            continue;
        }
        else
        {
            /*ֱ������*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*ƴ��sql�ļ���*/
        sprintf( sLineBufW , "%s(%s\'%s\'%c\'%s\'%c%d%c\'%s\');\n" , 
                           sLineHeadW, sLineBufWT, 
                           sLineBufR, VALUE_SEP_CHAR, 
                           sPk_id, VALUE_SEP_CHAR, 
                           channel_no, VALUE_SEP_CHAR,
                           szDailyDate );
        //SysLog( LOGTYPE_DEBUG , "[%s]" , sLineBufW );
        fprintf( fp_sql , sLineBufW );
    }
    
    SAFE_FCLOSE( fp );
    SAFE_FCLOSE( fp_sql );
    
    return NOERR;
}

/**
 *װ�ض����ļ���LFEE
 **/
int load_file_lfee( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*��ȡ�л���*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*д�л���*/
    char  sLineBufWT[4096]   = { 0 };    /*д�л���*/
    char  sPk_id[21]         = { 0 };    /*��ˮ�ļ���������*/
    FILE *fp  = NULL , *fp_sql = NULL;
    char  value[100]         = { 0 };
    int   channel_no      ;
    
    SysLog( LOGTYPE_DEBUG , "[%s]" , datafileallname );
    
    sprintf( sLineHeadW , 
            "insert into t_liquidate_file_lfee("
            "f_card_type, f_mcc, f_agent_code, f_snd_code, "
            "f_iss_code, f_rcv_code, f_msg_code, f_tran_code, "
            "f_sub_tran_code, f_trace_no, f_tran_time, f_card_no, "
            "f_org_info, f_terminal_no, f_merchant_no, f_tran_rrn, "
            "f_tran_amt, f_acq_fee, f_lfee, f_net_amt, "
            "f_data_lfee, f_id, f_channel_no, f_file_date) values"
    );
        
    /*���ļ�*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*���ļ�*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //���ж�ȡ�ļ�
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //�жϿ���
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*��ʽ�������ļ���*/
        iRet = func_fmt_line( LFEE, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ʽ�������ļ���ʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ȡ��ˮid*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_lfee_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ȡ��ˮidʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ˮ�������봦��*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(LFEE, sLineBufR, 14, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*��������*/
            channel_no = CHANNEL_CUP_DAIFU ;
        }
        else
        {
            /*ֱ������*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*ƴ��sql�ļ���*/
        sprintf( sLineBufW , "%s(%s\'%s\'%c\'%s\'%c%d%c\'%s\');\n" , 
                           sLineHeadW, sLineBufWT, 
                           sLineBufR, VALUE_SEP_CHAR, 
                           sPk_id, VALUE_SEP_CHAR, 
                           channel_no, VALUE_SEP_CHAR,
                           szDailyDate );
        fprintf( fp_sql , sLineBufW );
    }
    
    SAFE_FCLOSE( fp );
    SAFE_FCLOSE( fp_sql );
    
    return NOERR;
}

/**
 *װ�ز���ļ���ERRA
 **/
int load_file_erra( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*��ȡ�л���*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*д�л���*/
    char  sLineBufWT[4096]   = { 0 };    /*д�л���*/
    char  sPk_id[21]         = { 0 };    /*��ˮ�ļ���������*/
    FILE *fp  = NULL , *fp_sql = NULL;
    char  value[100]         = { 0 };
    int   channel_no     ;
    
    SysLog( LOGTYPE_DEBUG , "[%s]" , datafileallname );
    
    sprintf( sLineHeadW , 
            "insert into t_liquidate_file_erra("
            "f_err_type, "
            "f_agent_code, f_snd_code, f_trace_no, "
            "f_tran_time, f_card_no, f_tran_amt, f_msg_code, "
            "f_tran_code, f_mcc, f_terminal_no, "
            "f_org_tran_rrn, f_sub_tran_code, f_auth_no, f_rcv_code, "
            "f_iss_code, "
            "f_org_trace_no, f_resp_code, f_rcv_fee, f_pay_fee, f_err_reason, "
            "f_org_tran_time, f_org_clear_date, f_org_tran_amt, f_merchant_no, "
            "f_merchant_name_add, f_merchant_fee, "
            "f_data_erra, f_id, f_channel_no, f_file_date) values"
    );
        
    /*���ļ�*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*���ļ�*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //���ж�ȡ�ļ�
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //�жϿ���
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*��ʽ�������ļ���*/
        iRet = func_fmt_line( ERRA, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ʽ�������ļ���ʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ȡ��ˮid*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_erra_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ȡ��ˮidʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ˮ�������봦��*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ERRA, sLineBufR, 8, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*��������*/
            channel_no = CHANNEL_CUP_DAIFU ;
        }
        else
        {
            /*ֱ������*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*ƴ��sql�ļ���*/
        sprintf( sLineBufW , "%s(%s\'%s\'%c\'%s\'%c%d%c\'%s\');\n" , 
                           sLineHeadW, sLineBufWT, 
                           sLineBufR, VALUE_SEP_CHAR, 
                           sPk_id, VALUE_SEP_CHAR, 
                           channel_no, VALUE_SEP_CHAR,
                           szDailyDate );
        //SysLog( LOGTYPE_DEBUG , "[%s]" , sLineBufW );
        fprintf( fp_sql , sLineBufW );
    }
    
    SAFE_FCLOSE( fp );
    SAFE_FCLOSE( fp_sql );
    
    return NOERR;
}

/**
 *װ���̻������ļ���ZSUM
 **/
int load_file_zsum( MYSQL *conn_ptr, char *acq_code, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*��ȡ�л���*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*д�л���*/
    char  sLineBufWT[4096]   = { 0 };    /*д�л���*/
    char  sPk_id[21]         = { 0 };    /*��ˮ�ļ���������*/
    FILE *fp  = NULL , *fp_sql = NULL;
    
    SysLog( LOGTYPE_DEBUG , "[%s]" , datafileallname );
    
    sprintf( sLineHeadW , 
            "insert into t_liquidate_file_zsum("
            "f_merchant_no, f_merchant_name_addr, f_tran_amt, "
            "f_merchant_fee, f_clear_net_amt, "
            "f_data_zsum, f_id, f_snd_code, f_file_date) values"
    );
        
    /*���ļ�*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*���ļ�*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //���ж�ȡ�ļ�
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //�жϿ���
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*��ʽ�������ļ���*/
        iRet = func_fmt_line( ZSUM, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ʽ�������ļ���ʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ȡ��ˮid*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_zsum_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ȡ��ˮidʧ�ܣ�" );
            return ERROR;
        }
        
        /*ƴ��sql�ļ���*/
        sprintf( sLineBufW , "%s(%s\'%s\'%c\'%s\'%c\'%s\'%c\'%s\');\n" , 
                           sLineHeadW, sLineBufWT, 
                           sLineBufR, VALUE_SEP_CHAR, 
                           sPk_id, VALUE_SEP_CHAR, 
                           acq_code, VALUE_SEP_CHAR,
                           szDailyDate );
        fprintf( fp_sql , sLineBufW );
    }
    
    SAFE_FCLOSE( fp );
    SAFE_FCLOSE( fp_sql );
    
    return NOERR;
}

/**
 *װ���̻��ʽ𻮸��ļ���ZPSUM
 **/
int load_file_zpsum( MYSQL *conn_ptr, char *acq_code, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*��ȡ�л���*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*д�л���*/
    char  sLineBufWT[4096]   = { 0 };    /*д�л���*/
    char  sPk_id[21]         = { 0 };    /*��ˮ�ļ���������*/
    FILE *fp  = NULL , *fp_sql = NULL;
    
    SysLog( LOGTYPE_DEBUG , "[%s]" , datafileallname );
    
    sprintf( sLineHeadW , 
            "insert into t_liquidate_file_zpsum("
            "f_merchant_no, f_mcc, f_merchant_name_addr, "
            "f_bank_name, f_bank_union_no, f_bank_acct_no, f_bank_acct_name, "
            "f_merchant_area_code, f_tran_num, f_tran_amt, "
            "f_tran_fee, f_err_num, f_err_amt, f_err_fee, "
            "f_clear_amt, f_offset_amt, f_real_recorded, "
            "f_data_zpsum, f_id, f_snd_code, f_file_date) values"
    );
    
    /*���ļ�*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    /*���ļ�*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        if ( errno == 2 )
        {
            //�޴��ļ�
            SysLog( LOGTYPE_ERROR , "���̻��ʽ𻮸��ļ������账��" );
            SAFE_FCLOSE( fp );
            SAFE_FCLOSE( fp );
            return NOERR;
        }
        else
        {
            SysLog( LOGTYPE_ERROR , "�򿪽�����ˮ�ļ�ʧ�� error=[%d][%s]" , 
                    errno, strerror(errno) );
            SAFE_FCLOSE( fp );
            SAFE_FCLOSE( fp );
            return ERROR;
        }
    }
    
    while(!feof(fp))
    {
        //���ж�ȡ�ļ�
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //�жϿ���
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*��ʽ�������ļ���*/
        iRet = func_fmt_line( ZPSUM, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ʽ�������ļ���ʧ�ܣ�" );
            return ERROR;
        }
        
        /*��ȡ��ˮid*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_zpsum_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "��ȡ��ˮidʧ�ܣ�" );
            return ERROR;
        }
        
        /*ƴ��sql�ļ���*/
        sprintf( sLineBufW , "%s(%s\'%s\'%c\'%s\'%c\'%s\'%c\'%s\');\n" , 
                           sLineHeadW, sLineBufWT, 
                           sLineBufR, VALUE_SEP_CHAR, 
                           sPk_id, VALUE_SEP_CHAR, 
                           acq_code, VALUE_SEP_CHAR,
                           szDailyDate );
        //SysLog( LOGTYPE_DEBUG , "[%s]" , sLineBufW );
        fprintf( fp_sql , sLineBufW );
    }
    
    SAFE_FCLOSE( fp );
    SAFE_FCLOSE( fp_sql );
    
    return NOERR;
}
/**
 *�ļ�����ȡ����������״̬
 **/
static int load_resp_file_end( MYSQL *conn_ptr )
{
    char  szSql[2048]    = { 0 };
    int    iRet          = 0;
    
    sprintf( szSql , 
            " UPDATE t_liquidate_file t "
            " LEFT JOIN ( "
            "   SELECT "
            "     count(t1.f_id) AS F_file_cnt, "
            "     sum(t1.f_tran_amt) AS F_file_amt, "
            "     t1.f_file_date f_file_date, "
            "     t1.f_channel_no f_channel_no "
            "   FROM "
            "     t_liquidate_file_dtl t1 "
            "   GROUP BY "
            "     t1.f_file_date, t1.f_channel_no "
            " ) v ON t.f_file_date = v.f_file_date AND t.f_channel_no = v.f_channel_no "
            " SET t.F_file_cnt = ifnull(v.F_file_cnt, 0), "
            "  t.f_file_amt = ifnull(v.F_file_amt, 0.00), "
            "  t.f_load_time = DATE_FORMAT(SYSDATE(),'%%Y-%%m-%%d %%H:%%i:%%s'), "
            "  t.f_status = %d "
            " WHERE "
            "   t.F_file_date = \'%s\' "
            "   and t.f_status = %d "
            , LIQUIDATE_FILE_STATUS_LOAD_SUCC , szDailyDate 
            , LIQUIDATE_FILE_STATUS_GEN_SUCC
    );
    
    iRet = db_update( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet <= 0 )
    {
        return ERROR;
    }
    
    return NOERR;
}


static int load_all_acq_file(MYSQL *conn_ptr, char acq_list_arr[][12])
{
    char  sliquidate_file_name[255] = { 0 };
    char  sqlfile[255]              = { 0 };    /*��������ļ�ȫ��*/
    char  szDate6[7]                = { 0 };
    char  sfileDir[100]             = { 0 };
    
    int   i;
    
    strncpy( szDate6, szDailyDate + 2, 6 );
    
    for( i = 0; i < MAX_ACQ_LIST ; i++ )
    {
        SysLog( LOGTYPE_DEBUG , "���ͻ�������[%s]" , acq_list_arr[i] );
        if ( strcmp( acq_list_arr[i], "" ) == 0 )
        {
            break;
        }

        /*�ļ�Ŀ¼*/
        sprintf( sfileDir, "%s/%s/%s", getenv("LIQUIDATE_FILE_LOCAL"), acq_list_arr[i], szDailyDate );
        
        /*װ���̻�������ˮ�ļ�*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s01ACOM", sfileDir, szDate6);
        /*sql�ļ�*/
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_acomn(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*װ���ļ������ݿ�*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "ִ��sql�ű�" ) ;
        
        /*װ���̻�������ˮ�ļ�*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s32ACOMA", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_acoma(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*װ���ļ������ݿ�*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "ִ��sql�ű�" ) ;
        
        /*װ�ػ�Ʒ�Ʒ�����ļ�*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s99ALFEE", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_lfee(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*װ���ļ������ݿ�*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "ִ��sql�ű�" ) ;

        /*װ�ز����ˮ�ļ�*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s32AERRA", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_erra(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*װ���ļ������ݿ�*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "ִ��sql�ű�" ) ;
        
        /*װ���̻���������ļ�*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/INO%s32ZSUM", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_zsum(conn_ptr, acq_list_arr[i], sliquidate_file_name, sqlfile) , "" ) ;
        /*װ���ļ������ݿ�*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "ִ��sql�ű�" ) ;
        
        /*װ���̻��ʽ𻮸��ļ�*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/INO%s99ZPSUM", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_zpsum(conn_ptr, acq_list_arr[i], sliquidate_file_name, sqlfile) , "" ) ;
        /*װ���ļ������ݿ�*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "ִ��sql�ű�" ) ;
    }
    
    return NOERR;
}

/**
 *װ�ض����ļ�
 **/
static int load_resp_file( MYSQL *conn_ptr )
{
    char acq_list_arr[MAX_ACQ_LIST][12];
    int i;
    
    /*�����ʼ��*/
    for ( i = 0; i < MAX_ACQ_LIST; i++ )
        memset( acq_list_arr[i], 0x00, sizeof( acq_list_arr[i] ) );

    CHECK ( db_get_acq_list(conn_ptr, acq_list_arr) , "��ȡ������������б�" );
    
    CHECK ( load_all_acq_file(conn_ptr, acq_list_arr) , "װ�����л��������ļ�" );

    return NOERR;
}

/**
 *��ʼ��
 **/
int files_load_init( MYSQL *conn_ptr )
{
    char  szSql[2048]    = { 0 };
    int   iRet               = -1;
    
    SysLog(  LOGTYPE_INFO , "��ʼ���������˵�" ) ;  
    
    sprintf( szSql , 
            " update t_liquidate_file t "
            "    set t.f_status = %d, "
            "        t.f_load_time = \'\', "
            "        t.F_file_cnt = 0, "
            "        t.f_file_amt = 0.00 "
            "  where t.f_file_date = \'%s\' "
            , LIQUIDATE_FILE_STATUS_GEN_SUCC
            , szDailyDate
    );
    
    iRet = db_update( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ɾ��������ˮ*/
    memset(szSql, 0x00, sizeof(szSql));  
    sprintf( szSql , 
            " delete from t_liquidate_file_dtl "
            "  where f_file_date = \'%s\' "
            , szDailyDate
    );
    iRet = db_delete( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ɾ������������ˮ*/
    memset(szSql, 0x00, sizeof(szSql));  
    sprintf( szSql , 
            " delete from t_liquidate_file_acoma "
            "  where f_file_date = \'%s\' "
            , szDailyDate
    );
    iRet = db_delete( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ɾ��Ʒ�Ʒ������ˮ*/
    memset(szSql, 0x00, sizeof(szSql));  
    sprintf( szSql , 
            " delete from t_liquidate_file_lfee "
            "  where f_file_date = \'%s\' "
            , szDailyDate
    );\
    iRet = db_delete( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ɾ�������ˮ*/
    memset(szSql, 0x00, sizeof(szSql));  
    sprintf( szSql , 
            " delete from t_liquidate_file_erra "
            "  where f_file_date = \'%s\' "
            , szDailyDate
    );
    iRet = db_delete( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ɾ���̻����������ˮ*/
    memset(szSql, 0x00, sizeof(szSql));  
    sprintf( szSql , 
            " delete from t_liquidate_file_zsum "
            "  where f_file_date = \'%s\' "
            , szDailyDate
    );
    iRet = db_delete( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    /*ɾ���̻��ʽ𻮸���ˮ*/
    memset(szSql, 0x00, sizeof(szSql));  
    sprintf( szSql , 
            " delete from t_liquidate_file_zpsum "
            "  where f_file_date = \'%s\' "
            , szDailyDate
    );
    iRet = db_delete( conn_ptr , szSql, LOG_SQL_TITLE );
    if ( iRet < 0 )
    {
        return ERROR;
    }
    
    SysLog(  LOGTYPE_INFO , "��ʼ���������˵�����" ) ;  

    return NOERR;
}

int func_load_resp_file( MYSQL *conn_ptr )
{
    int   iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "װ���ʽ�ͨ�������ļ���ʼ����" );
    
    get_dayend_date( szDailyDate );
    
    iRet = files_load_init( conn_ptr );
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "��ʼ��ʧ��" );
        return STEP_EXEC_STATUS_FAIL;
    }

    /*װ�ض����ļ�*/
    iRet = load_resp_file( conn_ptr );
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "װ�ض����ļ�ʧ�ܡ���" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*װ���ļ����*/
    iRet = load_resp_file_end( conn_ptr );
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "װ���ļ�����ʧ�ܡ���" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    SysLog( LOGTYPE_INFO , "װ���ʽ�ͨ�������ļ���������" );
    
    return STEP_EXEC_STATUS_SUCC;
}