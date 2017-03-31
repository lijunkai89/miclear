include ${HOME}/src/Makefile.defines
MAKE=make
MAKEOPT=
ALLDIRS= \
	${HOME}/src/pub \
	${HOME}/src/dbbase \
	${HOME}/src/dbpub \
	${HOME}/src/main\
	${HOME}/src/dayendstart \
	${HOME}/src/syncjounal \
	${HOME}/src/gendirtclrfile \
	${HOME}/src/loadrespfile \
	${HOME}/src/checkrespfile \
	${HOME}/src/inserttrace \
	${HOME}/src/merchantstat \
	${HOME}/src/fundchnlstat \
	${HOME}/src/dayendend \


          
all:
	@for subdir in ${ALLDIRS}; \
	do \
		cd $$subdir; \
		echo "Makeing $$subdir"; \
		${MAKE} ${MAKEOPT} all; \
		echo "Done\n\n"; \
	done

clean:
	@for subdir in ${ALLDIRS}; \
	do \
		cd $$subdir; \
		echo "Makeing $$subdir"; \
		${MAKE} ${MAKEOPT} clean; \
		echo "Done\n\n"; \
	done

