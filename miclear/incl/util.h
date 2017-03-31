#ifndef __UTIL_H__
#define __UTIL_H__
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

#endif

char softKey[33] = "1234567890ABCDEF1234567890ABCDEF";

extern short  Hex2Asc(char *Hex, char *asc, int HexLen);

extern short  Asc2Hex( char * asc, char * Hex, int AscLen);

extern int DataDES(char *p_pin_key, char *p_in, char *p_out,char flag );