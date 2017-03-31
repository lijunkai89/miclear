include ${HOME}/src/Makefile.defines


OBJS=fundchnlstat.o
TARGET=${LIBDIR}/libfundchnlstat.so
LIBAPPEND= -llog -lpub -ldbbase
#PROCOUTS=dbpub.c

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET} ${PROCOUTS}
	
${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND} ${ORALIBS}
