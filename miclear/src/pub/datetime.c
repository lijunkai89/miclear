/****************************************************************************
 *    Copyright (c) :小米-小米支付.                      *
 *    ProgramName : 公共c函数
 *    SystemName  : 日期时间函数
 *    Version     : 1.0
 *    Language    : C
 *    OS & Env    : CentOS Linux release 7.2.1511 (Core)
                    mysql  Ver 14.14 Distrib 5.6.35, for Linux (x86_64)
 *    Description : 清分清算系统-公共C函数
 *    History     :
 *    YYYY/MM/DD        Position        Author         Description
 *    ------------------------------------------------------------------------
 *    2017/03/21         北京           李君凯         创建文档
******************************************************************************/
#include <sys/timeb.h>
#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>


int	Year( long Date )
{
	return ( Date / 10000 );
}

int	Month( long Date )
{
	return ( Date / 100 % 100 );
}

int	Day( long Date )
{
	return ( Date % 100 );
}

/**
 *获取本地日期
 **/
long  _FunGetDate()
{
    long                 iTime;
    struct tm            *T;
    char                 sDate[15];
    
    time(&iTime);
    T=localtime(&iTime);
    snprintf(sDate
            ,sizeof(sDate)
            ,"%04d%02d%02d"
            ,T->tm_year+1900
            ,T->tm_mon+1
            ,T->tm_mday
    );
    
    return atol(sDate);
}

/**
 *获取本地时间
 **/
long  _FunGetTime()
{
    long                 iTime;
    struct tm           *T;
    char                 sTime[15];
    
    time(&iTime);
    T=localtime(&iTime);
    snprintf(sTime
            ,sizeof(sTime)
            ,"%02d%02d%02d"
            ,T->tm_hour
            ,T->tm_min+1
            ,T->tm_sec
    );
    
    return atol(sTime);
}

/**
 *拼接年月日成日期
 **/
long ToDate( int Y, int M, int D )
{
    if ( M < 1 || M > 12 ) {
       M --;
       Y += M / 12 + ( M % 12 + 12 ) / 12 - 1;
       M = ( M % 12 + 12 ) % 12 + 1;
    }
    while ( D <= -DayInYear( Y - 1 ) ) {
       Y --;
       D += DayInYear( Y );
    }
    while ( D < 1 ) {
       M --;
       if ( M == 0 ) {
 	  Y --;
	  M = 12;
       }
       D += DayInMonth( Y, M );
    }
    while ( D > DayInYear( Y ) ) {
       D -= DayInYear( Y );
       Y ++;
    }
    while ( D > DayInMonth( Y, M ) ) {
       D -= DayInMonth( Y, M );
       M ++;
       if ( M == 13 ) {
          Y ++;
	  M = 1;
       }
    }
    return ( ( long ) Y * 10000 + M * 100 + D );
}

/**
 *计算年有几天
 **/
int DayInYear( int Y )
{
    if ( Y % 4 == 0 && ( Y % 100 != 0 || Y % 400 == 0 ) )
       return 366;
    return 365;
}

/**
 *计算月有几天
 **/
int DayInMonth( int Y, int M )
{
    int	d[12] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    
    if ( M != 2 )
       return d[ M - 1 ];
    if ( Y % 4 == 0 && ( Y % 100 != 0 || Y % 400 == 0 ) )
       return 29;
    return 28;
}

/**
 *根据日期差计算日期
 **/
long DateAfter( long Date, int Days )
{
    return ToDate( Year( Date ), Month( Date ), Day( Date ) + Days );
}

/**
 *字符串日期转长整形日期
 **/
long LongDate( char *date )
{
    long longDate;
   
    longDate = atol(date);
    return longDate;
}

/**
 *长整形日期转字符串日期
 **/
void CharDate( long date, char *NewDate )
{
    memset( NewDate, 0x00, 9 );
    sprintf( NewDate, "%4ld%02ld%02ld", date / 10000, date % 10000 / 100,
		date % 100 );
}

/**
 *计算下一日
 **/
void DateAdd( char *sDate, int iDays )
{
    CharDate( DateAfter( LongDate( sDate ), iDays ), sDate );
}

/**
 *根据日期差计算日期
 **/
char * _FunDateAdd( char *sDate, int iDays )
{
    CharDate( DateAfter( LongDate( sDate ), iDays ), sDate );
    sDate[8] = 0x0;
    return sDate;
}