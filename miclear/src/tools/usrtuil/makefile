include ${HOME}/src/Makefile.defines


OBJS=usrutil.o
TARGET=${LIBDIR}/libusrutil.so
LIBAPPEND= -ldes3


all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET}

${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND}
