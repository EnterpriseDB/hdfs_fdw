.PHONY : clean

ifdef USE_PGXS
PG_INC_PATH=$(shell pg_config --includedir-server)
CPPFLAGS= -Wno-unused-variable -fPIC -Wall -g -I$(PG_INC_PATH) -I$(JDK_INCLUDE) -I$(JDK_INCLUDE)/linux/ -Ijdbc
else
subdir = contrib/hdfs_fdw/libhive
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
override CPPFLAGS += -Wno-unused-variable -fPIC -Wall -g -I$(JDK_INCLUDE) -I$(JDK_INCLUDE)/linux/ -Ijdbc
endif
LDFLAGS:= $(LDFLAGS) -shared

JDBC =	 jdbc/hiveclient.cpp jdbc/data.cpp

SOURCES = $(JDBC)
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

