include ${HOME}/src/Makefile.defines
MAKE=make
MAKEOPT=
ALLDIRS= \
	${HOME}/src/tools/log \
	${HOME}/src/tools/des3 \
	${HOME}/src/tools/usrtuil \
	${HOME}/src/tools/dbset \


          
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

