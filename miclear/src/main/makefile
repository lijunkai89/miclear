include ${HOME}/src/Makefile.defines


OBJS=dayend.o
TARGET=${BINDIR}/dayend
LIBAPPEND= -ldl -ldbpub -lpub -llog 


all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${LIB} ${LIBAPPEND} ${MYSQLLIBS}
