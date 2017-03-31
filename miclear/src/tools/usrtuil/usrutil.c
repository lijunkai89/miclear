/*-
 * Copyright (C), 1988-2011, Nantian Co., Ltd.
 *
 * vi:set ts=4 sw=4:
 */
#ifndef lint
static const char rcsid[] = "$Id: util.c,v 1.2.2.3 2011/06/29 18:17:36 mymtom Exp $";
#endif /* not lint */

/**
 * @file	util.c
 * @brief	
 */

#include <ctype.h>

#include "util.h"
#include "des3.h"


/*
��16���Ƶ�����ת����asc��
*/
short  Hex2Asc(char *Hex, char *asc, int HexLen)
{
   int i;
   char *AscPtr = asc;
   char *HexPtr = Hex;
   char Temp;
   for(i = 0; i < HexLen; i++)
   {
      Temp = (*HexPtr & 0xf0) >> 4;
      if (Temp < 10)
         *AscPtr = 0x30 + Temp;
      else
         *AscPtr = 0x37 + Temp;
      AscPtr++;
      Temp = *HexPtr & 0x0f;
      if (Temp < 10)
         *AscPtr = 0x30 + Temp;
      else
         *AscPtr = 0x37 + Temp;
      AscPtr++;
      HexPtr++;
   }

   Hex = HexPtr;

   return 0;
}


/*
��asc����ת����16���Ƶ�����ֻ����'0'-'9','a'-'f'
��Ҫ��֤��������ݱ����ǿ���ת���ģ����ܳ��������Χ��������������
*/

short  Asc2Hex( char * asc, char * Hex, int AscLen)
{
   char * HexPtr = Hex;
   short i;

   for(i = 0; i < AscLen; i++)
   {
      /*A:0x41(100 0001),a:0x61(110 0001),����4λ����0001,��0x90��0xa*/

      *HexPtr = asc[i] << 4;
      if (!(asc[i] >= '0' && asc[i] <= '9' ))
         *HexPtr += (char)0x90;
      i++;
      *HexPtr |= (asc[i] & 0x0F);
      if (!(asc[i] >= '0' && asc[i] <= '9' ))
      *HexPtr += 0x09;
      HexPtr++;
   }
   return 0;
}



/*
 * ˵����3des����
 * ���룺p_pin_key:��Կ  p_in:����
 * �����p_out ����
 */
int DataDES(char *p_pin_key, char *p_in, char *p_out,char flag )
{
	char s_pin_key[64];
	char s_in[64];
	char s_tmp[32];
	int len_key=0;
	int len_in=0;
	int i;
	
	memset(s_pin_key, 0, sizeof(s_pin_key));
	memset(s_in, 0, sizeof(s_in));
	memset(s_tmp, 0, sizeof(s_tmp));
	
	len_key=strlen(p_pin_key);
	len_in=strlen(p_in);
   if(len_key%8 ||len_in%8||len_in<=0||len_key<=0)
   {
   	  printf("��Կ�����ĳ��ȱ���Ϊ8�ı����Ҵ���0\n");
   	  return -1;
   }
   if(len_key!=32)
   {
   	 printf("��Կ����ӦΪ32λ");
   	 return -1;
   }
   
	 Asc2Hex( p_pin_key, s_pin_key, len_key );
	 if(flag=='0')/*����*/ 
     desDkey( (unsigned char*)s_pin_key, EN0 ); 
   else if(flag=='1')/*����*/
   	 desDkey( (unsigned char*)s_pin_key, DE1 ); 
   else
   {
   	 printf("��Ч����ʽ");
   	 return -1;
   }
   Asc2Hex( p_in, s_in, len_in);

  for(i=0;i<len_in/2;i=i+8)
  {
 	 memset(s_tmp, 0, sizeof(s_tmp) );
   Ddes( (unsigned char*)s_in+i, (unsigned char*)s_tmp);
   Hex2Asc(s_tmp,p_out+i*2, 8);
  }
  return 0;     
}



