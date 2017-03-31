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
#define  SIGMA  10L

/*ftpδ��ȡ�ļ�ʱ�ĵȴ���������*/
#define WAIT_MAX_TIMES  72  /*���ȴ�����*/
#define SLEEP_SECONDS   600  /*ÿ�εȴ�ʱ�䣨�룩*/

#define MAX_ACQ_LIST  35  /*���������������*/

/*������ƴ���ļ���*/
//#define FILE_PREFIX  "IND"
//#define FILE_COMA_SUFFIX  "32ACOMA"  /*ACOMA�ļ���׺*/
//#define FILE_ERRA_SUFFIX  "32AERRA"  /*AERRA�ļ���׺*/

/*�ļ��ֶηָ���*/
#define  FTP_FILE_SPLITCHAR  ' '        /*�������ո�*/
#define  BOC_FILE_DELIM      '\t'      /*���У��Ʊ��*/

/* ��������״̬ */
#define  LIQUIDATE_FILE_STATUS_INIT      0 
#define  LIQUIDATE_FILE_STATUS_GEN_SUCC      1 
#define  LIQUIDATE_FILE_STATUS_LOAD_SUCC      2 
//#define  LIQUIDATE_FILE_STATUS_CHKING      3 
#define  LIQUIDATE_FILE_STATUS_CHK_SUCC      4 
#define  LIQUIDATE_FILE_STATUS_CHK_FAIL      5 
#define  LIQUIDATE_FILE_STATUS_DELETE      9 

/* ��ˮ���˱�ʶ */
#define  FUNDCHNL_CHK_STATUS_INIT      "0"   /*δ����*/
#define  FUNDCHNL_CHK_STATUS_OK        "1"   /*�������*/
#define  FUNDCHNL_CHK_STATUS_MORE      "2"   /*ϵͳ���˲�*/
#define  FUNDCHNL_CHK_STATUS_LESS      "3"   /*ϵͳ������*/
#define  FUNDCHNL_CHK_STATUS_NOTEQL    "5"   /*����*/
#define  FUNDCHNL_CHK_STATUS_NONEED    "4"   /*�������*/
#define  FUNDCHNL_CHK_STATUS_DEL       "6"   /*ɾ��*/

/*����������������*/
#define FUNDCHNL_CHK_MAX_DIFF_DAYS  2  /*���ȴ�����*/

/*�������˲��-���Ǽ�����*/
#define FUNDCHNL_CHK_ERR_GEN_TYPE_SYS  0  /*ϵͳ�Զ�*/
#define FUNDCHNL_CHK_ERR_GEN_TYPE_MAN  1  /*�˹�¼��*/

/* �������˲��-���̿��ʶ */
#define  FUNDCHNL_CHK_ERR_LONG         1  /*����*/
#define  FUNDCHNL_CHK_ERR_SHORT        2  /*�̿�*/
#define  FUNDCHNL_CHK_ERR_OTHER        3  /*����*/

/* �������˲��-״̬ */
#define  FUNDCHNL_CHK_ERR_STATUS_INIT         0  /*δ����*/
#define  FUNDCHNL_CHK_ERR_STATUS_TO_BAL       1  /*����תƽ��*/
#define  FUNDCHNL_CHK_ERR_STATUS_DEAL         2  /*�����Ѵ���*/

//----------------------------- �������� ---------------------------------------------------//
#define		APP_LOGIN		 					80 	 	/* �ն�ǩ��        	                */
#define		APP_LOGIN_DES	 					801 	/* �ն�ǩ��        	                */
#define		APP_LOGIN_3DES	 					802 	/* �ն�ǩ��        	                */
#define		APP_LOGIN_TDK	 					803 	/* �ն�ǩ��        	                */
#define		APP_LOGOUT							81		/* �ն�ǩ��        	                */
#define		APP_SETTLE							82 		/* �ն˽���        	                */
#define		APP_BATCH							83 		/* ������        	                */
#define		APP_BATCH_OFFLINE	 				831 	/* �ѻ�����        	  #��ƽ�ѻ����� */
#define		APP_BATCH_ONLINE_B	 				832 	/* ��������        	  #ƽ���������� */
#define		APP_BATCH_ONLINE_U	 				833 	/* ��������        	  #��ƽ�������� */
#define		APP_BATCH_ERROR_B	 				834 	/* �쳣����        	  #ƽ���쳣���� */
#define		APP_BATCH_ERROR_U	 				835 	/* �쳣����        	  #��ƽ�쳣���� */
#define		APP_BATCH_OVER_B	 				836 	/* ���ͽ���        	  #��ƽ���ͽ��� */
#define		APP_BATCH_OVER_U	 				837 	/* ���ͽ���        	  #ƽ�����ͽ��� */
#define		APP_STATUS	 						84  	/* ״̬����        	                */
#define		APP_STATUS_NORMAL	 			    841  	/* ��ͨ״̬����        	            */
#define		APP_STATUS_ES 						842  	/* ����ǩ������        	            */
#define		APP_PARA	 						85 		/* ��������        	  #��������     */
#define		APP_PARA_DOWN	 					851 	/* ��������        	  #��������     */
#define		APP_PARA_OVER	 					852 	/* ��������        	  #��������     */
#define		APP_DOWN_JFBBIN						853 	/* ��������  	      #���ֱ�����   */
#define		APP_ECHO							86		/* �������        	                */
#define		APP_ICKEY							87 		/* ��Կ����        	                */
#define		APP_ICKEY_QUERY						871 	/* ��Կ��ѯ        	                */
#define		APP_ICKEY_DOWN						872 	/* ��Կ����        	                */
#define		APP_ICKEY_OVER						873 	/* ��Կ���ؽ���    	                */
#define		APP_ICPARA							88 		/* IC��������      	                */
#define		APP_ICPARA_QUERY					881 	/* IC������ѯ      	                */
#define		APP_ICPARA_DOWN						882 	/* IC��������      	                */
#define		APP_ICPARA_OVER						883 	/* IC�������ؽ���  	                */
#define		APP_HMD_DOWN						884 	/* ����������    	                */
#define		APP_HMD_OVER						885 	/* ���������ؽ��� 	                */
#define		APP_INIT							89	 	/* POS��ʼ��	  	                */
#define		APP_INIT_HS							891	 	/* ��ʼ������	  	                */
#define		APP_INIT_DOWN_TMPKEY				892	 	/* ��ʼ��ʱ��Կ 	                */
#define		APP_INIT_DOWN_MKEY					893	 	/* ��ʼ������Կ 	                */
#define		APP_ICSCRIPT					  	90 		/* �ű�����		     ,              */
#define		APP_ICSCRIPT_PURCHASE_OFFLINE	  	9039 	/* �ѻ����ѽű�      ,              */
#define		APP_ICSCRIPT_PURCHASE_NORMAL	  	9031 	/* ��ͨ���ѽű�		                */
#define		APP_ICSCRIPT_PURCHASE_ORDER		  	9032 	/* �������ѽű�		                */
#define		APP_ICSCRIPT_PREAUTH_NORMAL	  		9041 	/* ��ͨ��Ȩ�ű�		                */
#define		APP_ICSCRIPT_PREAUTH_ORDER	  		9042 	/* ������Ȩ�ű�		                */
#define		APP_ICSCRIPT_BALANCE			  	9091 	/* ����ѯ�ű�   	                */
#define		APP_BALANCE	  						91   	/* ����ѯ   		                */
#define		APP_PURCHASE						3		/* ����         	          		*/
#define		APP_PURCHASE_OFFLINE	  			39  	/* �ѻ�����       	          		*/
#define		APP_PURCHASE_NORMAL	  				31  	/* ��ͨ����			                */
#define		APP_PURCHASE_ORDER	  				32  	/* ��������			                */
#define		APP_PREAUTH							4   	/* Ԥ��Ȩ			                */
#define		APP_PREAUTH_NORMAL 					41  	/* ��ͨ��Ȩ			                */
#define		APP_PREAUTH_ORDER 					42  	/* ������Ȩ			                */
#define		APP_COMFIRM_NORMAL 	  				43  	/* ��ͨ���			                */
#define		APP_COMFIRM_ORDER					44  	/* �������			                */
#define		APP_REFUND	  						5 		/* �˻�				                */
#define		APP_REFUND_NORMAL			  		531 	/* ��ͨ�˻�			                */
#define		APP_REFUND_ORDER	 				532 	/* �����˻�			                */
#define		APP_CANCEL	  						7 		/* ����		                		*/
#define		APP_CANCEL_PURCHASE_NORMAL			731 	/* ��ͨ���ѳ���		                */
#define		APP_CANCEL_PURCHASE_ORDER	  		732 	/* �������ѳ���		                */
#define		APP_CANCEL_PREAUTH_NORMAL	 		741 	/* ��ͨ��Ȩ����		                */
#define		APP_CANCEL_PREAUTH_ORDER	 		742 	/* ������Ȩ����		                */
#define		APP_CANCEL_COMFIRM_NORMAL	 		743 	/* ��ͨ��ɳ���		                */
#define		APP_CANCEL_COMFIRM_ORDER	 		744 	/* ������ɳ���		                */
#define		APP_AUTOVOID						2 	 	/* ����		                		*/
#define		APP_AUTOVOID_PURCHASE_NORMAL		231 	/* ��ͨ���ѳ���		                */
#define		APP_AUTOVOID_PURCHASE_ORDER			232 	/* �������ѳ���		                */
#define		APP_AUTOVOID_PREAUTH_NORMAL			241 	/* ��ͨ��Ȩ����		                */
#define		APP_AUTOVOID_PREAUTH_ORDER			242 	/* ������Ȩ����		                */
#define		APP_AUTOVOID_COMFIRM_NORMAL			243 	/* ��ͨ��ɳ���		                */
#define		APP_AUTOVOID_COMFIRM_ORDER			244 	/* ������ɳ���		                */
#define		APP_AUTOVOID_CANCEL_PURCHASE_NORMAL	2731	/* ��ͨ���ѳ�������	                */
#define		APP_AUTOVOID_CANCEL_PURCHASE_ORDER	2732	/* �������ѳ�������	                */
#define		APP_AUTOVOID_CANCEL_PREAUTH_NORMAL	2741	/* ��ͨ��Ȩ��������	                */
#define		APP_AUTOVOID_CANCEL_PREAUTH_ORDER	2742	/* ������Ȩ��������	                */
#define		APP_AUTOVOID_CANCEL_COMFIRM_NORMAL	2743	/* ��ͨ��ɳ�������	                */
#define		APP_AUTOVOID_CANCEL_COMFIRM_ORDER	2744	/* ������ɳ�������	                */

/* �������䱨��-�������� */
#define MSG_TYPE_PRE         "0100"    /*Ԥ��Ȩ��Ԥ��Ȩ���*/
#define MSG_TYPE_PAY         "0200"    /*���ѡ����,���ѳ�������ɳ���*/
#define MSG_TYPE_REF         "0220"    /*�˻�*/
#define MSG_TYPE_REVE        "0420"    /*���ֳ���*/

/* �������䱨��-���״�����-3�� */
#define PRO_CODE_PAY         "000000"    /*���ѡ����*/
#define PRO_CODE_REVA        "200000"    /*���ѳ�������ɳ������˻�*/

/* ������ˮ��-������ */
#define CARD_TYPE_DEBIT         "01"    /*��ǿ�*/
#define CARD_TYPE_CREDIT        "02"    /*���ǿ�*/
#define CARD_TYPE_QUASI_CREDIT  "03"    /*׼���ǿ�*/
#define CARD_TYPE_PREPAID       "10"    /*׼���ǿ�*/

/* ������ˮ��-����״̬ */
#define TRAN_FLAG_FINISH    0  /*�������*/
#define TRAN_FLAG_RAVA      1  /*���ױ�����*/

/* ������ˮ��-������ʶ */
#define TRAN_SAF_FLAG_NOT    0  /*����δ����*/
#define TRAN_SAF_FLAG_YES    1  /*�����ѳ���*/

/*���㷵�ر�ʶ*/
#define RT_STL_NEED     1     /*��Ҫ����*/
#define RT_STL_NOTYET   0     /*�������*/
#define RT_ERR          -1    /*�жϽ�����Ϣʧ��*/

/* �������� */
#define STL_CYCLE_DAY     1   /*�ս�*/
#define STL_CYCLE_WEEK    2   /*�ܽ�*/
#define STL_CYCLE_MONTH   3   /*�½�*/
#define STL_CYCLE_QUARTER 4   /*���Ƚ�*/
#define STL_CYCLE_YEAR    5   /*���*/
#define STL_CYCLE_UNKNOWN 6   /*δ֪*/

/*�̻���������*/
#define MER_STL_TYPE_SELF       1      /*���Խ���*/
#define MER_STL_TYPE_PARENT     2      /*�鼯����*/


/*�����ˮ-�����б�ʶ*/
#define  PAYOUT_JOUNAL_BANK_SELF          0      /*����*/
#define  PAYOUT_JOUNAL_BANK_OTHER         1      /*����*/

/*�̻�������-��������*/
#define  FEE_STL_TYPE_ALL         1      /*ȫ��*/
#define  FEE_STL_TYPE_NET         2      /*����*/

/*Ŀ��ģ���*/
#define  CHANNEL_CMBC_QR            0      /*������΢��֧������*/
#define  CHANNEL_CUP_INDIRECT       1      /*��������*/
#define  CHANNEL_CUP_DIRECT         2      /*����ֱ��*/
#define  CHANNEL_CUP_DAIFU          3      /*������������*/
#define  CHANNEL_CUP_MANY           4      /*������*/

/*���������ʶ*/
#define  MER_LIN_TYPE_DIRECT         1      /*ֱ���̻�*/
#define  MER_LIN_TYPE_INDIRECT       2      /*�����̻�*/

/*�ⶥ��ʶ*/
#define  MER_BASE_FEE_NO_LIMIT       0      /*�޷ⶥ*/
#define  MER_BASE_FEE_LIMIT_MAX      1      /*�ⶥ*/

/*ѹ���ļ��ű���*/
static char FUND_FILE_ZIP_SH[50] = "deal_payout_files.sh";

/*�����ʶ*/
#define  DC_FLAG_D     'D'
#define  DC_FLAG_C     'C'

//�̻�����ģʽ
#define        SHOP_STL_MODE_FUND                              1         /* ͨ�����̻�                                */
#define        SHOP_STL_MODE_ACQ                               3         /* �յ����̻�                                */