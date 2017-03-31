include ${HOME}/src/Makefile.defines


OBJS=des3.o
TARGET=${LIBDIR}/libdes3.so

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED}
