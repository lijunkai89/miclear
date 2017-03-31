include ${HOME}/src/Makefile.defines


OBJS=pub.o datetime.o
TARGET=${LIBDIR}/libpub.so
LIBAPPEND= -llog


all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND}
