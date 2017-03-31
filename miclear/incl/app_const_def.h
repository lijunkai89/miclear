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
#define  SIGMA  10L

/*ftp未获取文件时的等待重做参数*/
#define WAIT_MAX_TIMES  72  /*最大等待次数*/
#define SLEEP_SECONDS   600  /*每次等待时间（秒）*/

#define MAX_ACQ_LIST  35  /*机构编码最大数量*/

/*银联：拼接文件名*/
//#define FILE_PREFIX  "IND"
//#define FILE_COMA_SUFFIX  "32ACOMA"  /*ACOMA文件后缀*/
//#define FILE_ERRA_SUFFIX  "32AERRA"  /*AERRA文件后缀*/

/*文件字段分隔符*/
#define  FTP_FILE_SPLITCHAR  ' '        /*银联：空格*/
#define  BOC_FILE_DELIM      '\t'      /*中行：制表符*/

/* 对账任务状态 */
#define  LIQUIDATE_FILE_STATUS_INIT      0 
#define  LIQUIDATE_FILE_STATUS_GEN_SUCC      1 
#define  LIQUIDATE_FILE_STATUS_LOAD_SUCC      2 
//#define  LIQUIDATE_FILE_STATUS_CHKING      3 
#define  LIQUIDATE_FILE_STATUS_CHK_SUCC      4 
#define  LIQUIDATE_FILE_STATUS_CHK_FAIL      5 
#define  LIQUIDATE_FILE_STATUS_DELETE      9 

/* 流水对账标识 */
#define  FUNDCHNL_CHK_STATUS_INIT      "0"   /*未对账*/
#define  FUNDCHNL_CHK_STATUS_OK        "1"   /*对账相符*/
#define  FUNDCHNL_CHK_STATUS_MORE      "2"   /*系统多账侧*/
#define  FUNDCHNL_CHK_STATUS_LESS      "3"   /*系统侧少账*/
#define  FUNDCHNL_CHK_STATUS_NOTEQL    "5"   /*金额不符*/
#define  FUNDCHNL_CHK_STATUS_NONEED    "4"   /*无需对账*/
#define  FUNDCHNL_CHK_STATUS_DEL       "6"   /*删除*/

/*银联对账最大差异日*/
#define FUNDCHNL_CHK_MAX_DIFF_DAYS  2  /*最大等待次数*/

/*银联对账差错单-差错登记类型*/
#define FUNDCHNL_CHK_ERR_GEN_TYPE_SYS  0  /*系统自动*/
#define FUNDCHNL_CHK_ERR_GEN_TYPE_MAN  1  /*人工录入*/

/* 银联对账差错单-长短款标识 */
#define  FUNDCHNL_CHK_ERR_LONG         1  /*长款*/
#define  FUNDCHNL_CHK_ERR_SHORT        2  /*短款*/
#define  FUNDCHNL_CHK_ERR_OTHER        3  /*其他*/

/* 银联对账差错单-状态 */
#define  FUNDCHNL_CHK_ERR_STATUS_INIT         0  /*未处理*/
#define  FUNDCHNL_CHK_ERR_STATUS_TO_BAL       1  /*错账转平账*/
#define  FUNDCHNL_CHK_ERR_STATUS_DEAL         2  /*错账已处理*/

//----------------------------- 交易类型 ---------------------------------------------------//
#define		APP_LOGIN		 					80 	 	/* 终端签到        	                */
#define		APP_LOGIN_DES	 					801 	/* 终端签到        	                */
#define		APP_LOGIN_3DES	 					802 	/* 终端签到        	                */
#define		APP_LOGIN_TDK	 					803 	/* 终端签到        	                */
#define		APP_LOGOUT							81		/* 终端签退        	                */
#define		APP_SETTLE							82 		/* 终端结算        	                */
#define		APP_BATCH							83 		/* 批上送        	                */
#define		APP_BATCH_OFFLINE	 				831 	/* 脱机批送        	  #不平脱机批送 */
#define		APP_BATCH_ONLINE_B	 				832 	/* 联机批送        	  #平衡联机批送 */
#define		APP_BATCH_ONLINE_U	 				833 	/* 联机批送        	  #不平联机批送 */
#define		APP_BATCH_ERROR_B	 				834 	/* 异常批送        	  #平衡异常批送 */
#define		APP_BATCH_ERROR_U	 				835 	/* 异常批送        	  #不平异常批送 */
#define		APP_BATCH_OVER_B	 				836 	/* 批送结束        	  #不平批送结束 */
#define		APP_BATCH_OVER_U	 				837 	/* 批送结束        	  #平衡批送结束 */
#define		APP_STATUS	 						84  	/* 状态上送        	                */
#define		APP_STATUS_NORMAL	 			    841  	/* 普通状态上送        	            */
#define		APP_STATUS_ES 						842  	/* 电子签名上送        	            */
#define		APP_PARA	 						85 		/* 参数传递        	  #参数传递     */
#define		APP_PARA_DOWN	 					851 	/* 参数传递        	  #参数传递     */
#define		APP_PARA_OVER	 					852 	/* 参数传递        	  #参数结束     */
#define		APP_DOWN_JFBBIN						853 	/* 卡表下载  	      #积分宝卡表   */
#define		APP_ECHO							86		/* 回响测试        	                */
#define		APP_ICKEY							87 		/* 公钥下载        	                */
#define		APP_ICKEY_QUERY						871 	/* 公钥查询        	                */
#define		APP_ICKEY_DOWN						872 	/* 公钥下载        	                */
#define		APP_ICKEY_OVER						873 	/* 公钥下载结束    	                */
#define		APP_ICPARA							88 		/* IC参数下载      	                */
#define		APP_ICPARA_QUERY					881 	/* IC参数查询      	                */
#define		APP_ICPARA_DOWN						882 	/* IC参数下载      	                */
#define		APP_ICPARA_OVER						883 	/* IC参数下载结束  	                */
#define		APP_HMD_DOWN						884 	/* 黑名单下载    	                */
#define		APP_HMD_OVER						885 	/* 黑名单下载结束 	                */
#define		APP_INIT							89	 	/* POS初始化	  	                */
#define		APP_INIT_HS							891	 	/* 初始化握手	  	                */
#define		APP_INIT_DOWN_TMPKEY				892	 	/* 初始临时密钥 	                */
#define		APP_INIT_DOWN_MKEY					893	 	/* 初始化主密钥 	                */
#define		APP_ICSCRIPT					  	90 		/* 脚本上送		     ,              */
#define		APP_ICSCRIPT_PURCHASE_OFFLINE	  	9039 	/* 脱机消费脚本      ,              */
#define		APP_ICSCRIPT_PURCHASE_NORMAL	  	9031 	/* 普通消费脚本		                */
#define		APP_ICSCRIPT_PURCHASE_ORDER		  	9032 	/* 订购消费脚本		                */
#define		APP_ICSCRIPT_PREAUTH_NORMAL	  		9041 	/* 普通授权脚本		                */
#define		APP_ICSCRIPT_PREAUTH_ORDER	  		9042 	/* 订购授权脚本		                */
#define		APP_ICSCRIPT_BALANCE			  	9091 	/* 余额查询脚本   	                */
#define		APP_BALANCE	  						91   	/* 余额查询   		                */
#define		APP_PURCHASE						3		/* 消费         	          		*/
#define		APP_PURCHASE_OFFLINE	  			39  	/* 脱机消费       	          		*/
#define		APP_PURCHASE_NORMAL	  				31  	/* 普通消费			                */
#define		APP_PURCHASE_ORDER	  				32  	/* 订购消费			                */
#define		APP_PREAUTH							4   	/* 预授权			                */
#define		APP_PREAUTH_NORMAL 					41  	/* 普通授权			                */
#define		APP_PREAUTH_ORDER 					42  	/* 订购授权			                */
#define		APP_COMFIRM_NORMAL 	  				43  	/* 普通完成			                */
#define		APP_COMFIRM_ORDER					44  	/* 订购完成			                */
#define		APP_REFUND	  						5 		/* 退货				                */
#define		APP_REFUND_NORMAL			  		531 	/* 普通退货			                */
#define		APP_REFUND_ORDER	 				532 	/* 订购退货			                */
#define		APP_CANCEL	  						7 		/* 撤销		                		*/
#define		APP_CANCEL_PURCHASE_NORMAL			731 	/* 普通消费撤销		                */
#define		APP_CANCEL_PURCHASE_ORDER	  		732 	/* 订购消费撤销		                */
#define		APP_CANCEL_PREAUTH_NORMAL	 		741 	/* 普通授权撤销		                */
#define		APP_CANCEL_PREAUTH_ORDER	 		742 	/* 订购授权撤销		                */
#define		APP_CANCEL_COMFIRM_NORMAL	 		743 	/* 普通完成撤销		                */
#define		APP_CANCEL_COMFIRM_ORDER	 		744 	/* 订购完成撤销		                */
#define		APP_AUTOVOID						2 	 	/* 冲正		                		*/
#define		APP_AUTOVOID_PURCHASE_NORMAL		231 	/* 普通消费冲正		                */
#define		APP_AUTOVOID_PURCHASE_ORDER			232 	/* 订购消费冲正		                */
#define		APP_AUTOVOID_PREAUTH_NORMAL			241 	/* 普通授权冲正		                */
#define		APP_AUTOVOID_PREAUTH_ORDER			242 	/* 订购授权冲正		                */
#define		APP_AUTOVOID_COMFIRM_NORMAL			243 	/* 普通完成冲正		                */
#define		APP_AUTOVOID_COMFIRM_ORDER			244 	/* 订购完成冲正		                */
#define		APP_AUTOVOID_CANCEL_PURCHASE_NORMAL	2731	/* 普通消费撤销冲正	                */
#define		APP_AUTOVOID_CANCEL_PURCHASE_ORDER	2732	/* 订购消费撤销冲正	                */
#define		APP_AUTOVOID_CANCEL_PREAUTH_NORMAL	2741	/* 普通授权撤销冲正	                */
#define		APP_AUTOVOID_CANCEL_PREAUTH_ORDER	2742	/* 订购授权撤销冲正	                */
#define		APP_AUTOVOID_CANCEL_COMFIRM_NORMAL	2743	/* 普通完成撤销冲正	                */
#define		APP_AUTOVOID_CANCEL_COMFIRM_ORDER	2744	/* 订购完成撤销冲正	                */

/* 银联网间报文-报文类型 */
#define MSG_TYPE_PRE         "0100"    /*预授权、预授权完成*/
#define MSG_TYPE_PAY         "0200"    /*消费、完成,消费撤销、完成撤销*/
#define MSG_TYPE_REF         "0220"    /*退货*/
#define MSG_TYPE_REVE        "0420"    /*各种冲正*/

/* 银联网间报文-交易处理码-3域 */
#define PRO_CODE_PAY         "000000"    /*消费、完成*/
#define PRO_CODE_REVA        "200000"    /*消费撤销、完成撤销、退货*/

/* 交易流水表-卡类型 */
#define CARD_TYPE_DEBIT         "01"    /*借记卡*/
#define CARD_TYPE_CREDIT        "02"    /*贷记卡*/
#define CARD_TYPE_QUASI_CREDIT  "03"    /*准贷记卡*/
#define CARD_TYPE_PREPAID       "10"    /*准贷记卡*/

/* 交易流水表-交易状态 */
#define TRAN_FLAG_FINISH    0  /*交易完成*/
#define TRAN_FLAG_RAVA      1  /*交易被撤销*/

/* 交易流水表-冲正标识 */
#define TRAN_SAF_FLAG_NOT    0  /*交易未冲正*/
#define TRAN_SAF_FLAG_YES    1  /*交易已冲正*/

/*结算返回标识*/
#define RT_STL_NEED     1     /*需要结算*/
#define RT_STL_NOTYET   0     /*无需结算*/
#define RT_ERR          -1    /*判断结算信息失败*/

/* 结算周期 */
#define STL_CYCLE_DAY     1   /*日结*/
#define STL_CYCLE_WEEK    2   /*周结*/
#define STL_CYCLE_MONTH   3   /*月结*/
#define STL_CYCLE_QUARTER 4   /*季度结*/
#define STL_CYCLE_YEAR    5   /*年结*/
#define STL_CYCLE_UNKNOWN 6   /*未知*/

/*商户结算类型*/
#define MER_STL_TYPE_SELF       1      /*各自结算*/
#define MER_STL_TYPE_PARENT     2      /*归集结算*/


/*打款流水-本他行标识*/
#define  PAYOUT_JOUNAL_BANK_SELF          0      /*本行*/
#define  PAYOUT_JOUNAL_BANK_OTHER         1      /*他行*/

/*商户手续费-结算类型*/
#define  FEE_STL_TYPE_ALL         1      /*全额*/
#define  FEE_STL_TYPE_NET         2      /*轧差*/

/*目标模块号*/
#define  CHANNEL_CMBC_QR            0      /*民生（微信支付宝）*/
#define  CHANNEL_CUP_INDIRECT       1      /*银联间连*/
#define  CHANNEL_CUP_DIRECT         2      /*银联直连*/
#define  CHANNEL_CUP_DAIFU          3      /*银联代付渠道*/
#define  CHANNEL_CUP_MANY           4      /*多渠道*/

/*代理清算标识*/
#define  MER_LIN_TYPE_DIRECT         1      /*直连商户*/
#define  MER_LIN_TYPE_INDIRECT       2      /*间连商户*/

/*封顶标识*/
#define  MER_BASE_FEE_NO_LIMIT       0      /*无封顶*/
#define  MER_BASE_FEE_LIMIT_MAX      1      /*封顶*/

/*压缩文件脚本名*/
static char FUND_FILE_ZIP_SH[50] = "deal_payout_files.sh";

/*借贷标识*/
#define  DC_FLAG_D     'D'
#define  DC_FLAG_C     'C'

//商户清算模式
#define        SHOP_STL_MODE_FUND                              1         /* 通道给商户                                */
#define        SHOP_STL_MODE_ACQ                               3         /* 收单给商户                                */