.PHONY : clean

THRIFT_HOME:="$(shell echo $(THRIFT_HOME))"
BOOST_HOME:="$(shell echo $(BOOST_HOME))"
INSTALL_DIR:="$(shell echo $(PGDIR))/lib"

CPPFLAGS= -Wno-unused-variable -fPIC -Wall -g -I$(THRIFT_HOME)/include -I$(THRIFT_HOME)/include/thrift -I$(BOOST_HOME)/include -I$(THRIFT_HOME)/include/thrift/fb303 -Ithrift -Imetastore -Iodbc -Iodbc2 -Iql
LDFLAGS= -shared -L$(THRIFT_HOME)/lib -lfb303 -lthrift

METASTORE = metastore/hive_metastore_constants.cpp  metastore/hive_metastore_types.cpp  metastore/ThriftHiveMetastore.cpp
THRIFT = thrift/hive_service_constants.cpp thrift/TCLIService_constants.cpp thrift/TCLIService_server.skeleton.cpp \
	 thrift/ThriftHive.cpp thrift/hive_service_types.cpp thrift/TCLIService.cpp thrift/TCLIService_types.cpp
ODBC =	 odbc/hiveclient.cpp odbc/hiveclienthelper.cpp \
	 odbc/HiveColumnDesc.cpp  odbc/HiveResultSet.cpp  odbc/HiveRowSet.cpp
QL = ql/queryplan_constants.cpp  ql/queryplan_types.cpp

SOURCES = $(THRIFT) $(METASTORE) $(QL) $(ODBC)
HEADERS = 
OBJECTS = $(SOURCES:.cpp=.o)

TARGET=libhive.so

all: $(TARGET)

clean:
	rm -f $(OBJECTS) $(TARGET)

install:
	cp -rf $(TARGET) $(INSTALL_DIR)

$(TARGET) : $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) -o $@ $(LDFLAGS)

