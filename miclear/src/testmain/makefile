include ${HOME}/src/Makefile.defines


OBJS=testmain.o
TARGET=${BINDIR}/testmain
LIBAPPEND= -ldbpub -lpub -llog 


all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${LIB} ${LIBAPPEND} ${MYSQLLIBS}
