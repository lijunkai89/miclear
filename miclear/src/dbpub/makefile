include ${HOME}/src/Makefile.defines


OBJS=dbpub.o
TARGET=${LIBDIR}/libdbpub.so
LIBAPPEND= -llog -lusrutil -ldbbase
#PROCOUTS=dbpub.c

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET} ${PROCOUTS}
	
${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND} ${MYSQLLIBS}
