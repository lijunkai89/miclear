# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

#----------------------------------------------ORACLE---------------------------------------------------#
export ORACLE_HOME=/oracle/product/11.2.0
export TNS_ADMIN=${HOME}/tnsnames
export NLS_LANG="Simplified Chinese_china".ZHS16GBK

#------------------------------------------ORACLE实例名-------------------------------------------------#
export ORACLE_SID=STL

#------------------------------------------PATH环境变量-------------------------------------------------#
PATH=/usr/bin:/usr/sbin:/sbin:/bin:/usr/local/bin:/usr/local/sbin:/etc:$HOME/bin:$HOME/script:/root/bin
PATH=${PATH}:${ORACLE_HOME}/bin:/usr/vac/bin:/usr/vacpp/bin:/usr/java6_64/bin:/usr/TEE-CLC-10.0.0
export PATH

#------------------------------------------LIBPATH环境变量----------------------------------------------#
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib:/usr/local/lib:${ORACLE_HOME}/lib:$HOME/lib
export LIBPATH=$LD_LIBRARY_PATH
export LANG=AMERICAN_AMERICA.ZHS16GBK

#------------------------------------------日志---------------------------------------------------------#
export LOGLEVEL=0
export LOGDIR=${HOME}/log

#---------------------------------机构代码--------------------------------------#
export HXT_AGENCY_CODE=48730000

#---------------------------------银联文件本地目录---------------------------------------#
export LOCAL_CUPFILES_DIR=${HOME}/datafile/fundchnl/cup/${HXT_AGENCY_CODE}

#--------------------------------打款文件目录--------------------------------------#
export REMOTE_PAYOUT_FILES_DIR=/opt/datafile/PAYOUT/WEBLOAD
export PAYOUT_FILES_DIR=${HOME}/datafile/PAYOUT

#--------------------------------对账文件下载目录--------------------------------------#
export REMOTE_CHECK_FILES_DIR=/opt/datafile/CHECKBOOK/WEBLOAD

#--------------------------------代理商对账文件下载目录--------------------------------------#
export REMOTE_AGENT_FILES_DIR=/opt/datafile/AGENT/WEBLOAD

alias mk='make clean;make'
alias cdb='cd ~/bin'
alias cdlog='cd ~/log'
alias cdlib='cd ~/lib'
alias cdi='cd ~/incl'
alias cds='cd ~/src'

PS1='$LOGIN $PWD> '
export PS1
clear

echo "--------------------------------------------------------------------------------------"
echo "|                         hxt日终批处理系统-开发环境                                 |"
echo "--------------------------------------------------------------------------------------"
