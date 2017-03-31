include ${HOME}/src/Makefile.defines


OBJS=checkrespfile.o
TARGET=${LIBDIR}/libcheckrespfile.so
LIBAPPEND= -llog -lpub -ldbpub
#PROCOUTS=dbpub.c

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET} ${PROCOUTS}
	
${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND} ${ORALIBS}
