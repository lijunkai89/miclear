include ${HOME}/src/Makefile.defines


OBJS=dbbase.o
TARGET=${LIBDIR}/libdbbase.so
LIBAPPEND= -llog -lusrutil
#PROCOUTS=dbpub.c

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET} ${PROCOUTS}
	
${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND} ${MYSQLLIBS}
