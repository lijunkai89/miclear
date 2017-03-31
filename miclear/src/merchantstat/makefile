include ${HOME}/src/Makefile.defines


OBJS=merchantstat.o
TARGET=${LIBDIR}/libmerchantstat.so
LIBAPPEND= -llog -lpub -ldbbase
#PROCOUTS=dbpub.c

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET} ${PROCOUTS}
	
${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND} ${ORALIBS}
