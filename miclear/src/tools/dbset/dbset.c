/*-
 *  * Copyright (C), 1988-2011, Nantian Co., Ltd.
 *   *
 *    * vi:set ts=4 sw=4:
 *     */
#ifndef lint
static const char rcsid[] = "$Id: dbconfig.c,v 1.2.2.1 2011/06/29 18:17:36 mymtom Exp $";
#endif /* not lint */

/**
 *  * @file	dbconfig.c
 *   * @brief	对连接数据库的用户名和密码加密
 *    */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include <util.h>
#include <mysql.h>

/**************************************************************
 *  ** 函数名      :  ShowMenu ()
 *   ** 功  能      :  将选择菜单显示
 *   ***************************************************************/
int ShowMenu()
{
    int choice = -1;
    char choicebuf[30];

    system("clear");

    printf("*************************数据库设置测试*************************\n");
    printf("	请选择功能：\n");
    printf("	[1]请输入用户名和密码\n");
    printf("	[2]平台数据库连接测试\n");
    printf("	[0]退出\n");
    printf("****************************************************************\n");
    printf("请选择：");
    fgets(choicebuf,sizeof(choicebuf),stdin);
    choice = atoi(choicebuf);
    printf("_________________________________________________________________\n");
    return choice;
}

/**************************************************************
 *  ** 函数名      :  SetDBconfig ()
 *   ** 功  能      :  设置连接数据库的用户名和密码
 *   ***************************************************************/
int SetDBconfig()
{
    char sUsername[128];
    char sPassword[128];
    char sEnname[128];
    char sEnpassword[128];
    char sFile[128];
    char sBuff[254];
    char buf[256];
    char tmp[128];
    size_t len;

    int iRet = -1;
    FILE *fp;

    memset(sUsername, 0, sizeof(sUsername));
    memset(sPassword, 0, sizeof(sPassword));
    memset(sEnname, 0, sizeof(sEnname));
    memset(sEnpassword, 0, sizeof(sEnpassword));


    printf("请输入连接平台数据库的用户名(不能超过16位):");
    fgets(sUsername,sizeof(sUsername),stdin);
    sUsername[16] = '0';
    
    printf("请输入连接平台数据库的的密码(不能超过16位):");
    fgets(sPassword,sizeof(sPassword),stdin);
    sPassword[16] = '0';

    memset(sEnpassword, 0x00, sizeof(sEnpassword));

    len = strlen(sUsername);
    if (len >= 1 && sUsername[len - 1] == '\n')
        sUsername[len - 1] = '\0';
    memset(buf, 0, sizeof(buf));
    Hex2Asc(sUsername, buf, 16);
    iRet = DataDES(softKey, buf, sEnname, '0');
    if (iRet != 0) {
        printf("用户名软加密失败，返回[%d]！！\n", iRet);
        return -1;
    }

    len = strlen(sPassword);
    if (len >= 1 && sPassword[len - 1] == '\n')
        sPassword[len - 1] = '\0';
    memset(buf, 0, sizeof(buf));
    Hex2Asc(sPassword, buf, 16);
    iRet = DataDES(softKey, buf, sEnpassword, '0');

    if (iRet != 0) {
        printf("密码软加密失败，返回[%d]！！\n", iRet);
        return -1;
    }

    memset(sFile,0x00,sizeof(sFile));
    sprintf(sFile,"%s/etc/dbset.ini",getenv("HOME"));

    fprintf(stdout,"加密后的,sEnname[%s],sEnpassword[%s]\n",sEnname,sEnpassword);


    if ((fp = fopen(sFile,"wb")) == NULL ) {
        fprintf(stdout,"S0000: 打开文件[%s]失败,errno=[%d](%s)",sFile,errno,strerror(errno));
        return -1;
    }

    memset(sBuff,0x00,sizeof(sBuff));
    sprintf(sBuff,"[CONFIG]\nusername=%s\npassword=%s\n",sEnname,sEnpassword);

    fwrite(sBuff,strlen(sBuff), 1,fp);
    fclose(fp);


    printf("设置连接数据库的用户名和密码成功!!\n");
    printf("请键入任意键返回......");
    getchar();
    return 0;

}


/**************************************************************
 *  ** 函数名      :  DBconnetct ()
 *   ** 功  能      :  数据库连接测试
 *   ***************************************************************/
int DBconnetct()
{
    char tmp[20];
    int iRet = -1;
    MYSQL  conn_ptr ;

    if ((iRet = dbopen(&conn_ptr))  != 0) {
        printf("打开数据库失败，错误码为iRet[%d]\n",iRet);
        printf("按键入任意键返回......\n");
        getchar();
        return -1;
    }
    dbclose(&conn_ptr);
    printf("打开数据库成功!\n");
    printf("按键入任意键返回......\n");
    getchar();

}


int main(int argc, char *argv[])
{
    int choice;
    char tmp[2];

    if ( isatty(STDIN_FILENO) == 0) {
        printf("standard input is not a terminal device!!\n");
        return -1;
    }
    while (1) {
        choice = ShowMenu();
        switch (choice) {
            /*1连接数据库的用户名和密码设置*/
        case 1:
            SetDBconfig();
            break;
            /*2.连接数据库测试*/
        case 2:
            DBconnetct();
            break;
        case 0:
            printf("退出\n");
            return 0;
        default:
            printf("请选择正确的功能!\n");
            getchar();
            break;
        }
    }
}

