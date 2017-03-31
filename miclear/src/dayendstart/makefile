include ${HOME}/src/Makefile.defines


OBJS=dayendstart.o
TARGET=${BINDIR}/dayendstart
LIBAPPEND= -ldl -ldbpub -lpub -llog -ldbbase 


all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${LIB} ${LIBAPPEND} ${MYSQLLIBS}
