include ${HOME}/src/Makefile.defines


OBJS=log.o
TARGET=${LIBDIR}/liblog.so

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED}