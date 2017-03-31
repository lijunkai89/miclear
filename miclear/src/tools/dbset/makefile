include ${HOME}/src/Makefile.defines


OBJS=dbset.o
TARGET=${BINDIR}/dbset
LIBAPPEND= -ldbpub -lusrutil -lpub


all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${LIB} ${LIBAPPEND}
