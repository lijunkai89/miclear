/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 清分清算系统
 *    SystemName  : 调度步骤-装载代付对账文件
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-装载代付对账文件
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/01/09         北京           李君凯         创建文档
******************************************************************************/
#include "appdef.h"
#include "sys_const_def.h"
#include "app_const_def.h"
#include "mysql.h"

#define  VALUE_SEP_CHAR  ','
#define LOG_SQL_TITLE "装载银联文件"
/*数据类型*/
#define  VALUE_DEF_TYPE_INT     'i'
#define  VALUE_DEF_TYPE_FLOAT     'f'
#define  VALUE_DEF_TYPE_STRING  'c'

#define  load_sql_sh  "load_mysql_sql_file.sh"

static char  szDailyDate[9]    = { 0 };

/*ACOMN文件需要入库字段列表*/
typedef struct 
{
    int  valueth;  /*在文件中的字段序号*/
    int  valuelen;  /*域长度*/
    int  getflag;  /*是否需要取出 0-不需要 1-需要*/
    char type;     /*是否需要取出 'c'-字符串 'i'-数值*/
}record_st;

record_st ACOMN[] = {
  { 1  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*代理机构标识码*/
  { 2  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*发送机构标识码*/
  { 3  , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*系统跟踪号*/
  { 4  , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易传输时间*/
  { 5  , 19 , 1 , VALUE_DEF_TYPE_STRING } ,  /*主张号*/
  { 6  , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*交易金额*/
  { 7  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*部分代收时的承兑金额*/
  { 8  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*持卡人交易手续费*/
  { 9  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*报文类型*/
  { 10 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易类型码*/
  { 11 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户类型*/
  { 12 , 8  , 1 , VALUE_DEF_TYPE_STRING } ,  /*终端号*/
  { 13 , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户号*/
  { 14 , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*检索参考号*/
  { 15 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*服务点条件码*/
  { 16 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*授权应答码*/
  { 17 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*接收机构标识码*/
  { 18 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*原始交易系统跟踪号*/
  { 19 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易返回码*/
  { 20 , 3  , 0 , VALUE_DEF_TYPE_STRING } ,  /*服务点输入方式*/
  { 21 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*受理方应收手续费*/
  { 22 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*受理方应付手续费*/
  { 23 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*转接服务费*/
  { 24 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*单双转换标识*/
  { 25 , 3  , 0 , VALUE_DEF_TYPE_STRING } ,  /*卡片序列号*/
  { 26 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*终端读取能力*/
  { 27 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*IC卡条件码*/
  { 28 , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*原始交易日期时间*/
  { 29 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*发卡机构标识码*/
  { 30 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*交易地区标识*/
  { 31 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*终端类型*/
  { 32 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*ECI标识*/
  { 33 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*分期付款附加手续费*/
  { 34 , 14 , 0 , VALUE_DEF_TYPE_STRING } ,  /*其他信息*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/*ACOMA文件需要入库字段列表*/
record_st ACOMA[] = {
  { 1  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*代理机构标识码*/
  { 2  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*发送机构标识码*/
  { 3  , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*系统跟踪号*/
  { 4  , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易传输时间*/
  { 5  , 19 , 1 , VALUE_DEF_TYPE_STRING } ,  /*主张号*/
  { 6  , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*交易金额*/
  { 7  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*部分代收时的承兑金额*/
  { 8  , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*持卡人交易手续费*/
  { 9  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*报文类型*/
  { 10 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易类型码*/
  { 11 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户类型*/
  { 12 , 8  , 1 , VALUE_DEF_TYPE_STRING } ,  /*终端号*/
  { 13 , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户号*/
  { 14 , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*检索参考号*/
  { 15 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*服务点条件码*/
  { 16 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*授权应答码*/
  { 17 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*接收机构标识码*/
  { 18 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*原始交易系统跟踪号*/
  { 19 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易返回码*/
  { 20 , 3  , 1 , VALUE_DEF_TYPE_STRING } ,  /*服务点输入方式*/
  { 21 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*受理方应收手续费*/
  { 22 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*受理方应付手续费*/
  { 23 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*转接服务费*/
  { 24 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*单双转换标识*/
  { 25 , 3  , 1 , VALUE_DEF_TYPE_STRING } ,  /*卡片序列号*/
  { 26 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*终端读取能力*/
  { 27 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*IC卡条件码*/
  { 28 , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*原始交易日期时间*/
  { 29 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*发卡机构标识码*/
  { 30 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*交易地区标识*/
  { 31 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*终端类型*/
  { 32 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*ECI标识*/
  { 33 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*分期付款附加手续费*/
  { 34 , 14 , 0 , VALUE_DEF_TYPE_STRING } ,  /*其他信息*/
  { 35 , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*发送方清算机构*/
  { 36 , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*接收方清算机构*/
  { 37 , 1  , 1 , VALUE_DEF_TYPE_STRING } ,  /*冲正标识*/
  { 38 , 1  , 1 , VALUE_DEF_TYPE_STRING } ,  /*撤销标识*/
  { 39 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*清算日期*/
  { 40 , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*清算场次*/
  { 41 , 40 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户名称地址*/
  { 42 , 3  , 0 , VALUE_DEF_TYPE_STRING } ,  /*交易币种*/
  { 43 , 9  , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*银联代理清算收单机构自动折扣手续费*/
  { 44 , 9  , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*商户手续费*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/*LFE文件需要入库字段列表*/
record_st LFEE[] = {
  { 1  , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*受理方一级分行代码*/
  { 2  , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*是否银联标准卡*/
  { 3  , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*姐贷记卡标识*/
  { 4  , 2  , 0 , VALUE_DEF_TYPE_STRING } ,  /*终端类型*/
  { 5  , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*卡介质*/
  { 6  , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*交易地域标识*/
  { 7  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户类型*/
  { 8  , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*受理方二级分行代码*/
  { 9  , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*受理机构代码*/
  { 10 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*转发机构代码*/
  { 11 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*发卡机构代码*/
  { 12 , 11 , 1 , VALUE_DEF_TYPE_STRING } ,  /*接收机构代码*/
  { 13 , 11 , 0 , VALUE_DEF_TYPE_STRING } ,  /*收单机构代码*/
  { 14 , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*报文类型*/
  { 15 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易处理码*/
  { 16 , 2  , 1 , VALUE_DEF_TYPE_STRING } ,  /*服务点条件码*/
  { 17 , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*系统跟踪号*/
  { 18 , 10 , 1 , VALUE_DEF_TYPE_STRING } ,  /*交易传输日期时间*/
  { 19 , 19 , 1 , VALUE_DEF_TYPE_STRING } ,  /*主账号*/
  { 20 , 28 , 0 , VALUE_DEF_TYPE_STRING } ,  /*转出卡*/
  { 21 , 28 , 0 , VALUE_DEF_TYPE_STRING } ,  /*转入卡*/
  { 22 , 42 , 1 , VALUE_DEF_TYPE_STRING } ,  /*原始交易信息*/
  { 23 , 8  , 1 , VALUE_DEF_TYPE_STRING } ,  /*受卡方终端标识码*/
  { 24 , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*受卡方标识码*/
  { 25 , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*检索参考号*/
  { 26 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*交易金额*/
  { 27 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*受理方手续费*/
  { 28 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*品牌服务费*/
  { 29 , 12 , 0 , VALUE_DEF_TYPE_STRING } ,  /*保留使用*/
  { 30 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*净金额*/
  { 31 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*交易发起方式*/
  { 32 , 99 , 0 , VALUE_DEF_TYPE_STRING } ,  /*保留使用*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/*ERRA文件需要入库字段列表*/
record_st ERRA[] = {
  { 1  , 3  , 1 , VALUE_DEF_TYPE_STRING } , /*差错类型*/
  { 2  , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*代理机构标识码*/
  { 3  , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*发送机构标识码*/
  { 4  , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*系统跟踪号*/
  { 5  , 10 , 1 , VALUE_DEF_TYPE_STRING } , /*交易传输时间*/
  { 6  , 19 , 1 , VALUE_DEF_TYPE_STRING } , /*主账号*/
  { 7  , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*交易金额*/
  { 8  , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*报文类型*/
  { 9  , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*交易类型码*/
  { 10 , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*商户类型*/
  { 11 , 8  , 1 , VALUE_DEF_TYPE_STRING } , /*受卡机终端标识码*/
  { 12 , 12 , 1 , VALUE_DEF_TYPE_STRING } , /*上一笔交易检索参考号*/
  { 13 , 2  , 1 , VALUE_DEF_TYPE_STRING } , /*服务点条件码*/
  { 14 , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*授权应答码*/
  { 15 , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*接收机构标识码*/
  { 16 , 11 , 1 , VALUE_DEF_TYPE_STRING } , /*发卡银行标识码*/
  { 17 , 6  , 1 , VALUE_DEF_TYPE_STRING } , /*上一笔交易系统跟踪号*/
  { 18 , 2  , 1 , VALUE_DEF_TYPE_STRING } , /*交易返回码*/
  { 19 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*服务点输入方式*/
  { 20 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*受理方应收手续费*/
  { 21 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*受理方应付手续费*/
  { 22 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*分期付款附加手续费*/
  { 23 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*持卡人交易手续费*/
  { 24 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*应收费用*/
  { 25 , 12 , 0 , VALUE_DEF_TYPE_FLOAT } , /*应付费用*/
  { 26 , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*差错原因*/
  { 27 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*转出机构标识码*/
  { 28 , 19 , 0 , VALUE_DEF_TYPE_STRING } , /*转出账号*/
  { 29 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*转入机构标识码*/
  { 30 , 19 , 0 , VALUE_DEF_TYPE_STRING } , /*转入账号*/
  { 31 , 10 , 1 , VALUE_DEF_TYPE_STRING } , /*上一笔的日期时间*/
  { 32 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*卡片序列号*/
  { 33 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*终端读取能力*/
  { 34 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*IC卡条件代码*/
  { 35 , 4  , 1 , VALUE_DEF_TYPE_STRING } , /*上一笔交易清算日起*/
  { 36 , 12 , 1 , VALUE_DEF_TYPE_FLOAT } , /*上一笔交易金额*/
  { 37 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*交易地域标识*/
  { 38 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*ECI标识*/
  { 39 , 15 , 1 , VALUE_DEF_TYPE_STRING } , /*商户代码*/
  { 40 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*发送方清算机构*/
  { 41 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*转出方清算机构*/
  { 42 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*转入方清算机构*/
  { 43 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*上一笔交易终端类型*/
  { 44 , 40 , 1 , VALUE_DEF_TYPE_STRING } , /*商户名称地址*/
  { 45 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*特殊计费类型*/
  { 46 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*特殊计费档次*/
  { 47 , 8  , 0 , VALUE_DEF_TYPE_STRING } , /*保留使用*/
  { 48 , 24 , 0 , VALUE_DEF_TYPE_STRING } , /*卡产品标识信息*/
  { 49 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*引发差错交易的最原始的交易的交易代码*/
  { 50 , 1  , 0 , VALUE_DEF_TYPE_STRING } , /*交易发起方*/
  { 51 , 2  , 0 , VALUE_DEF_TYPE_STRING } , /*账户结算类型*/
  { 52 , 46 , 0 , VALUE_DEF_TYPE_STRING } , /*保留使用*/
  { 53 , 3  , 0 , VALUE_DEF_TYPE_STRING } , /*原始交易类型*/
  { 54 , 9  , 1 , VALUE_DEF_TYPE_FLOAT } , /*商户手续费*/
  { 55 , 11 , 0 , VALUE_DEF_TYPE_STRING } , /*商户结算行*/
  { 56 , 9  , 0 , VALUE_DEF_TYPE_FLOAT } , /*商户结算行费用*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

record_st ZSUM[] = {
  { 1  , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户代码*/
  { 2  , 80 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户全程*/
  { 3  , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*交易金额*/
  { 4  , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*手续费金额*/
  { 5  , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*清算金额*/
  { 6  , 200 , 0 , VALUE_DEF_TYPE_STRING } ,  /*保留使用*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

record_st ZPSUM[] = {
  { 1  , 15 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户代码*/
  { 2  , 4  , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户类型*/
  { 3  , 80 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户全称*/
  { 4  , 60 , 1 , VALUE_DEF_TYPE_STRING } ,  /*开户行名称*/
  { 5  , 12 , 1 , VALUE_DEF_TYPE_STRING } ,  /*开户行支付系统行号*/
  { 6  , 12 , 0 , VALUE_DEF_TYPE_STRING } ,  /*开户行清算号*/
  { 7  , 32 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户账号*/
  { 8  , 80 , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户账户名*/
  { 9  , 6  , 1 , VALUE_DEF_TYPE_STRING } ,  /*商户所属地区编码*/
  { 10 , 8  , 1 , VALUE_DEF_TYPE_INT } ,  /*正常交易笔数*/
  { 11 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*正常交易金额*/
  { 12 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*正常交易费用金额*/
  { 13 , 8  , 1 , VALUE_DEF_TYPE_INT } ,  /*差错交易笔数*/
  { 14 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*差错交易金额*/
  { 15 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*差错交易回退费用金额*/
  { 16 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*其他资金总额*/
  { 17 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*清算汇总金额*/
  { 18 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*收支两条线费用抵减金额*/
  { 19 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*收支两条线费用抵减方*/
  { 20 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*本期正常释放金额*/
  { 21 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*本期正常挂账金额*/
  { 22 , 13 , 0 , VALUE_DEF_TYPE_FLOAT } ,  /*本期回补金额*/
  { 23 , 13 , 1 , VALUE_DEF_TYPE_FLOAT } ,  /*入账金额*/
  { 24 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*入账责任方*/
  { 25 , 1  , 0 , VALUE_DEF_TYPE_STRING } ,  /*回补类型*/
  { 26 , 198 , 0 , VALUE_DEF_TYPE_STRING } ,  /*保留使用*/
  { -1 , -1 , 0 , VALUE_DEF_TYPE_STRING } ,
};

/**
 *按长度拆分字符串
 *参数说明：
 *      pread    字符串
 *      valuelen 字段长度
 *      buf       字段值
 *      delin     分隔符
 *返回值：
 *      拆分后的字符串
 **/
char *_fun_cut_fileline( char *pread , char *buf , int valuelen , char delin )
{
    char *pret = NULL;
    
    if ( (pread + valuelen)[0] != delin )
    {
        /*行末尾无分隔符*/
        if ( strlen( pread ) < valuelen )
        {
            SysLog( LOGTYPE_ERROR , "分隔符有误[%c]" , (pread + valuelen)[0] );
        }
    }
    
    pret = pread + valuelen + 1;
    memset( buf , 0x00 , 50 );
    strncpy( buf , pread , valuelen );
    
    return pret;
}

/**
 *正常取值
 *入参：value_in
 *      data_type
 *出惨：value_out
 *返回值：
 *说明：收款交易，所有金额正值显示（+付款类的交易金额）
 *      付款交易，所有金额负值显示（除明确应收应付含义的字段外）
 **/
int deal_data_type(char *value_in, char *value_out, char data_type)
{
    //字符类型的处理
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
 *获得流水id
 *取带引号的参数
 *入参：value_in
 *      data_type
 *出惨：value_sql
 *返回值：
 *说明：收款交易，所有金额正值显示（+付款类的交易金额）
 *      付款交易，所有金额负值显示（除明确应收应付含义的字段外）
 **/
void deal_data_type_for_sql(char *value_in, char *value_sql, char data_type)
{
    //字符类型的处理
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
 *处理从文件读取的数据行
 *返回处理后的数据长度
 **/
int deal_data_line( char *sLineBufR )
{
    if ( 0 == strlen(sLineBufR) )
    {
        return 0;
    }
    
    //行尾去换行符“\n”
    if ( 10 == sLineBufR[strlen(sLineBufR) - 1] )
    {
        sLineBufR[strlen(sLineBufR) - 1] = '\0';
    }
    //行尾去回车符"\r"
    if ( 13 == sLineBufR[strlen(sLineBufR) - 1] )
    {
        sLineBufR[strlen(sLineBufR) - 1] = '\0';
    }
    
    return strlen( sLineBufR );
}

/*执行sql脚本*/
int func_load_file( MYSQL *conn_ptr, char *sqlfile )
{
    FILE *fp_sql  = NULL;
    char  sLineBufR[4096]    = { 0 };     /*读取行缓冲*/
    int   iRet               = -1;
        
    /*打开文件*/
    if ( ( fp_sql = fopen( sqlfile , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开sql脚本文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp_sql))
    {
        //逐行读取文件
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        fgets( sLineBufR , sizeof(sLineBufR) , fp_sql ); 
        
        //判断空行
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
 *格式化文件行
 **/
int func_fmt_line( record_st *record_st, char *sLineBufR, char *sLineBufWT)
{
    char  *pread = NULL ;                     /*读取，写入行指针*/
    char  sFieldBuf[100]          =  { 0 };    /*阈值行指针*/
    char  sFieldBufT[100]          =  { 0 };    /*阈值行指针*/
    int  i  = 0;                /*计数器-字段个数*/
    
    //数据行换行处理
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
            //字符类型的处理
            deal_data_type_for_sql(sFieldBufT, sFieldBuf, record_st[i].type);
            
            strcat( sLineBufWT , sFieldBuf );
            sLineBufWT[strlen(sLineBufWT)] = VALUE_SEP_CHAR ;
        }
    }
    
    return NOERR;
}

/**
 *根据id找到指定域值
 **/
int get_value_by_id( record_st *record_st, char *sLineBufR, int id, char *value )
{
    char  *pread = NULL ;                     /*读取，写入行指针*/
    char  sFieldBuf[100]          =  { 0 };    /*阈值行指针*/
    char  sFieldBufT[100]          =  { 0 };    /*阈值行指针*/
    int  i  = 0;                /*计数器-字段个数*/
    
    //行尾去换行符“\n”
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
 *装载对账文件：acomn
 **/
int load_file_acomn( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*读取行缓冲*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*写行缓冲*/
    char  sLineBufWT[4096]   = { 0 };    /*写行缓冲*/
    char  sPk_id[21]         = { 0 };    /*流水文件导入主键*/
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
        
    /*打开文件*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*打开文件*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //逐行读取文件
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //判断空行
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*格式换对账文件行*/
        iRet = func_fmt_line( ACOMN, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "格式换对账文件行失败！" );
            return ERROR;
        }
        
        /*ECI标识----线上线下拆分*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMN, sLineBufR, 32, value);
        SysLog( LOGTYPE_DEBUG , "[%s]" , value );
        if ( strcmp( value, " " ) == 0 )
        {
            /*线下*/
            continue;
        }
        
        /*获取流水id*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_dtl_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "获取流水id失败！" );
            return ERROR;
        }
        
        /*流水渠道分离处理*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMN, sLineBufR, 10, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*代付渠道*/
            channel_no = CHANNEL_CUP_DAIFU;
        }
        else if ( strcmp( value, "300000" ) == 0 )
        {
            continue;
        }
        else
        {
            /*直连渠道*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*拼接sql文件行*/
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
 *装载对账文件：acoma
 **/
int load_file_acoma( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*读取行缓冲*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*写行缓冲*/
    char  sLineBufWT[4096]   = { 0 };    /*写行缓冲*/
    char  sPk_id[21]         = { 0 };    /*流水文件导入主键*/
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
        
    /*打开文件*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*打开文件*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //逐行读取文件
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //判断空行
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*格式换对账文件行*/
        iRet = func_fmt_line( ACOMA, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "格式换对账文件行失败！" );
            return ERROR;
        }
        
        /*ECI标识----线上线下拆分*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMN, sLineBufR, 32, value);
        SysLog( LOGTYPE_DEBUG , "[%s]" , value );
        if ( strcmp( value, "  " ) != 0 )
        {
            /*线下*/
            continue;
        }
        
        /*获取流水id*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_acoma_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "获取流水id失败！" );
            return ERROR;
        }
        
        /*流水渠道分离处理*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ACOMA, sLineBufR, 10, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*代付渠道*/
            channel_no = CHANNEL_CUP_DAIFU ;
        }
        else if ( strcmp( value, "300000" ) == 0 )
        {
            continue;
        }
        else
        {
            /*直连渠道*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*拼接sql文件行*/
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
 *装载对账文件：LFEE
 **/
int load_file_lfee( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*读取行缓冲*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*写行缓冲*/
    char  sLineBufWT[4096]   = { 0 };    /*写行缓冲*/
    char  sPk_id[21]         = { 0 };    /*流水文件导入主键*/
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
        
    /*打开文件*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*打开文件*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //逐行读取文件
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //判断空行
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*格式换对账文件行*/
        iRet = func_fmt_line( LFEE, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "格式换对账文件行失败！" );
            return ERROR;
        }
        
        /*获取流水id*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_lfee_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "获取流水id失败！" );
            return ERROR;
        }
        
        /*流水渠道分离处理*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(LFEE, sLineBufR, 14, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*代付渠道*/
            channel_no = CHANNEL_CUP_DAIFU ;
        }
        else
        {
            /*直连渠道*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*拼接sql文件行*/
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
 *装载差错文件：ERRA
 **/
int load_file_erra( MYSQL *conn_ptr, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*读取行缓冲*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*写行缓冲*/
    char  sLineBufWT[4096]   = { 0 };    /*写行缓冲*/
    char  sPk_id[21]         = { 0 };    /*流水文件导入主键*/
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
        
    /*打开文件*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*打开文件*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //逐行读取文件
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //判断空行
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*格式换对账文件行*/
        iRet = func_fmt_line( ERRA, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "格式换对账文件行失败！" );
            return ERROR;
        }
        
        /*获取流水id*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_erra_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "获取流水id失败！" );
            return ERROR;
        }
        
        /*流水渠道分离处理*/
        memset( value, 0x00, sizeof( value ) );
        get_value_by_id(ERRA, sLineBufR, 8, value);
        if ( strcmp( value, "290000" ) == 0 )
        {
            /*代付渠道*/
            channel_no = CHANNEL_CUP_DAIFU ;
        }
        else
        {
            /*直连渠道*/
            channel_no = CHANNEL_CUP_DIRECT ;
        }
        
        /*拼接sql文件行*/
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
 *装载商户汇总文件：ZSUM
 **/
int load_file_zsum( MYSQL *conn_ptr, char *acq_code, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*读取行缓冲*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*写行缓冲*/
    char  sLineBufWT[4096]   = { 0 };    /*写行缓冲*/
    char  sPk_id[21]         = { 0 };    /*流水文件导入主键*/
    FILE *fp  = NULL , *fp_sql = NULL;
    
    SysLog( LOGTYPE_DEBUG , "[%s]" , datafileallname );
    
    sprintf( sLineHeadW , 
            "insert into t_liquidate_file_zsum("
            "f_merchant_no, f_merchant_name_addr, f_tran_amt, "
            "f_merchant_fee, f_clear_net_amt, "
            "f_data_zsum, f_id, f_snd_code, f_file_date) values"
    );
        
    /*打开文件*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        return ERROR;
    }
    /*打开文件*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    
    while(!feof(fp))
    {
        //逐行读取文件
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //判断空行
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*格式换对账文件行*/
        iRet = func_fmt_line( ZSUM, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "格式换对账文件行失败！" );
            return ERROR;
        }
        
        /*获取流水id*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_zsum_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "获取流水id失败！" );
            return ERROR;
        }
        
        /*拼接sql文件行*/
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
 *装载商户资金划付文件：ZPSUM
 **/
int load_file_zpsum( MYSQL *conn_ptr, char *acq_code, char *datafileallname, char *sqlfile )
{
    int   iRet               = -1;
    char  sLineBufR[4096]    = { 0 };     /*读取行缓冲*/
    char  sLineHeadW[2048]   = { 0 };
    char  sLineBufW[4096]    = { 0 };    /*写行缓冲*/
    char  sLineBufWT[4096]   = { 0 };    /*写行缓冲*/
    char  sPk_id[21]         = { 0 };    /*流水文件导入主键*/
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
    
    /*打开文件*/
    if ( ( fp_sql = fopen( sqlfile , "w" ) ) == NULL )
    {
        SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                errno, strerror(errno) );
        SAFE_FCLOSE( fp_sql );
        return ERROR;
    }
    /*打开文件*/
    if ( ( fp = fopen( datafileallname , "r" ) ) == NULL )
    {
        if ( errno == 2 )
        {
            //无此文件
            SysLog( LOGTYPE_ERROR , "无商户资金划付文件，无需处理" );
            SAFE_FCLOSE( fp );
            SAFE_FCLOSE( fp );
            return NOERR;
        }
        else
        {
            SysLog( LOGTYPE_ERROR , "打开交易流水文件失败 error=[%d][%s]" , 
                    errno, strerror(errno) );
            SAFE_FCLOSE( fp );
            SAFE_FCLOSE( fp );
            return ERROR;
        }
    }
    
    while(!feof(fp))
    {
        //逐行读取文件
        memset( sLineBufR , 0x00 , sizeof( sLineBufR ) );
        memset( sLineBufW , 0x00 , sizeof( sLineBufW ) );
        memset( sLineBufWT , 0x00 , sizeof( sLineBufWT ) );
        
        fgets( sLineBufR , sizeof(sLineBufR) , fp );  

        //判断空行
        if ( 0 == deal_data_line(sLineBufR) )
        {
            break;
        }
        
        /*格式换对账文件行*/
        iRet = func_fmt_line( ZPSUM, sLineBufR, sLineBufWT );
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "格式换对账文件行失败！" );
            return ERROR;
        }
        
        /*获取流水id*/
        iRet = gen_dtl_id(conn_ptr, "seq_liquidate_file_zpsum_id", sPk_id);
        if ( NOERR != iRet )
        {
            SysLog( LOGTYPE_ERROR , "获取流水id失败！" );
            return ERROR;
        }
        
        /*拼接sql文件行*/
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
 *文件导入取，更新任务状态
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
    char  sqlfile[255]              = { 0 };    /*导入控制文件全名*/
    char  szDate6[7]                = { 0 };
    char  sfileDir[100]             = { 0 };
    
    int   i;
    
    strncpy( szDate6, szDailyDate + 2, 6 );
    
    for( i = 0; i < MAX_ACQ_LIST ; i++ )
    {
        SysLog( LOGTYPE_DEBUG , "发送机构编码[%s]" , acq_list_arr[i] );
        if ( strcmp( acq_list_arr[i], "" ) == 0 )
        {
            break;
        }

        /*文件目录*/
        sprintf( sfileDir, "%s/%s/%s", getenv("LIQUIDATE_FILE_LOCAL"), acq_list_arr[i], szDailyDate );
        
        /*装载商户对账流水文件*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s01ACOM", sfileDir, szDate6);
        /*sql文件*/
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_acomn(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*装载文件到数据库*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "执行sql脚本" ) ;
        
        /*装载商户清算流水文件*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s32ACOMA", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_acoma(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*装载文件到数据库*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "执行sql脚本" ) ;
        
        /*装载户品牌服务费文件*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s99ALFEE", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_lfee(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*装载文件到数据库*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "执行sql脚本" ) ;

        /*装载差错流水文件*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/IND%s32AERRA", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_erra(conn_ptr, sliquidate_file_name, sqlfile) , "" ) ;
        /*装载文件到数据库*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "执行sql脚本" ) ;
        
        /*装载商户清算汇总文件*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/INO%s32ZSUM", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_zsum(conn_ptr, acq_list_arr[i], sliquidate_file_name, sqlfile) , "" ) ;
        /*装载文件到数据库*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "执行sql脚本" ) ;
        
        /*装载商户资金划付文件*/
        memset( sliquidate_file_name, 0x00, sizeof( sliquidate_file_name ) );
        sprintf( sliquidate_file_name, "%s/INO%s99ZPSUM", sfileDir, szDate6);
        sprintf( sqlfile, "%s.sql" , sliquidate_file_name );
        CHECK ( load_file_zpsum(conn_ptr, acq_list_arr[i], sliquidate_file_name, sqlfile) , "" ) ;
        /*装载文件到数据库*/
        CHECK ( func_load_file(conn_ptr, sqlfile) , "执行sql脚本" ) ;
    }
    
    return NOERR;
}

/**
 *装载对账文件
 **/
static int load_resp_file( MYSQL *conn_ptr )
{
    char acq_list_arr[MAX_ACQ_LIST][12];
    int i;
    
    /*数组初始化*/
    for ( i = 0; i < MAX_ACQ_LIST; i++ )
        memset( acq_list_arr[i], 0x00, sizeof( acq_list_arr[i] ) );

    CHECK ( db_get_acq_list(conn_ptr, acq_list_arr) , "获取受理机构代码列表" );
    
    CHECK ( load_all_acq_file(conn_ptr, acq_list_arr) , "装载所有机构清算文件" );

    return NOERR;
}

/**
 *初始化
 **/
int files_load_init( MYSQL *conn_ptr )
{
    char  szSql[2048]    = { 0 };
    int   iRet               = -1;
    
    SysLog(  LOGTYPE_INFO , "初始化银联对账单" ) ;  
    
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
    
    /*删除清算流水*/
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
    
    /*删除代理清算流水*/
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
    
    /*删除品牌服务费流水*/
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
    
    /*删除差错流水*/
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
    
    /*删除商户清算汇总流水*/
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
    
    /*删除商户资金划付流水*/
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
    
    SysLog(  LOGTYPE_INFO , "初始化银联对账单结束" ) ;  

    return NOERR;
}

int func_load_resp_file( MYSQL *conn_ptr )
{
    int   iRet          = -1;
    
    SysLog( LOGTYPE_INFO , "装载资金通道对账文件开始……" );
    
    get_dayend_date( szDailyDate );
    
    iRet = files_load_init( conn_ptr );
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "初始化失败" );
        return STEP_EXEC_STATUS_FAIL;
    }

    /*装载对账文件*/
    iRet = load_resp_file( conn_ptr );
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "装载对账文件失败……" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    /*装载文件完成*/
    iRet = load_resp_file_end( conn_ptr );
    if ( NOERR != iRet )
    {
        SysLog( LOGTYPE_ERROR , "装载文件结束失败……" );
        return STEP_EXEC_STATUS_FAIL;
    }
    
    SysLog( LOGTYPE_INFO , "装载资金通道对账文件结束……" );
    
    return STEP_EXEC_STATUS_SUCC;
}