include ${HOME}/src/Makefile.defines


OBJS=gendirtclrfile.o
TARGET=${LIBDIR}/libgendirtclrfile.so
LIBAPPEND= -llog -lpub -ldbpub
#PROCOUTS=dbpub.c

all:${TARGET}
clean:
	@- ${RM} ${OBJS} ${TARGET} ${PROCOUTS}
	
${TARGET}:${OBJS}
	${CC} -o ${TARGET} ${OBJS} ${INCL} ${SHARED} ${LIB} ${LIBAPPEND} ${ORALIBS}
