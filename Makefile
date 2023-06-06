
#note: for whatever reason, Ryan's makefile wasn't linking everything properly,
#			 so we used my makefile from the previous Sprint --Ishan
#note: We broke up several files in heap_storage into separate .h and .cpp files 
#			 to prevent linking issues
#
INCLUDE_DIR = /usr/local/db6/include
LIB_DIR     = /usr/local/db6/lib

OBJS       =  storage_engine.o SlottedPage.o HeapFile.o HeapTable.o heap_storage.o EvalPlan.o ParseTreeToString.o SchemaTables.o SQLExec.o  cpsc4300.o

cpsc4300: $(OBJS)
	g++ -L$(LIB_DIR) $(OBJS) -ldb_cxx -lsqlparser -o $@


storage_engine.o : storage_engine.h 
SlottedPage.o: SlottedPage.h 
HeapFile.o: HeapFile.h
HeapTable.o: HeapTable.h 
heap_storage.o: heap_storage.h
ParseTreeToString.o : ParseTreeToString.h
SchemaTables.o : SchemaTables.h
SQLExec.o : SQLExec.h
EvalPlan.o: EvalPlan.h
cpsc4300.o: cpsc4300.cpp


# General rule for compilation
%.o: %.cpp *.h
	g++ -I$(INCLUDE_DIR) -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -o "$@" "$<" -ldb_cxx -lsqlparser


clean:
	rm -f cpsc300 *.o
