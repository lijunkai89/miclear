# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
        . ~/.bashrc
fi

# User specific environment and startup programs

#------------------------------------------PATH环境变量-------------------------------------------------#
#export MYSQL_HOST=localhost
export MYSQL_HOST=10.235.136.252
#export MYSQL_NAME=miclear
export MYSQL_NAME=mi_pospay_acq
export MYSQL_POSP_NAME=paps_t_db_zx

#------------------------------------------PATH环境变量-------------------------------------------------#
PATH=$PATH:$HOME/.local/bin:$HOME/bin:/usr/bin:/usr/sbin:/sbin:/bin:/usr/local/bin:/usr/local/sbin:/etc:$HOME/script:/root/bin
#PATH=${PATH}:/usr/vac/bin:/usr/vacpp/bin:/usr/java6_64/bin:/usr/TEE-CLC-10.0.0
export PATH

#------------------------------------------LIBPATH环境变量----------------------------------------------#
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib:/usr/local/lib:$HOME/lib:/usr/lib64:/usr/lib64/mysql
export LIBPATH=$LD_LIBRARY_PATH
#export LANG=AMERICAN_AMERICA.ZHS16GBK
export LANG=zh_CN.UTF-8
#------------------------------------------日志---------------------------------------------------------#
export LOGLEVEL=0
export LOGDIR=${HOME}/log

#------------------------------------------文件路劲-------------------------------------------------------#
export LIQUIDATE_FILE_LOCAL=${HOME}/datafile/liquidatefile 

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
echo "|                      小米支付-清算系统-开发环境                                    |"
echo "--------------------------------------------------------------------------------------"
