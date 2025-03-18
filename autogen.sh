#! /bin/bash

#-------------------------------------------------------------------------
#
# autogen.sh
#             Foreign-data wrapper for remote Hadoop/Hive servers
#
# Portions Copyright (c) 2004-2025, EnterpriseDB Corporation.
#
# IDENTIFICATION
#             autogen.sh
#
#-------------------------------------------------------------------------

if [ "$#" -ne 1 ]; then
    echo "Usage: autogen.sh thrift-install-dir"
    exit
fi


# Download and install thrift
function configure_thrift
{
	rm -rf thrift-0.9.2.tar.gz
	wget http://mirrors.gigenet.com/apache/thrift/0.9.2/thrift-0.9.2.tar.gz
	tar -zxvf thrift-0.9.2.tar.gz
	cd thrift-0.9.2
	export CXXFLAGS="-fPIC"
	./configure --prefix=$1 -with-qt4=no --with-c_glib=no --with-csharp=no --with-java=no --with-erlang=no --with-nodejs=no --with-lua=no --with-python=no --with-perl=no --with-php=no --with-php_extension=no --with-ruby=no --with-haskell=no --with-go=no --with-d=no
	make
	make install
	cd ..
}

# Install thrift's fb303
function configure_fb303
{
	cd thrift-0.9.2/contrib/fb303
	
	export CXXFLAGS="-fPIC"
	./bootstrap.sh
	./configure --prefix=$1
	make
	make install
	cd ../../../
}

# Compile and install libhive
function configure_libhive
{
	cd libhive
	make
	make install
}

configure_thrift
configure_fb303
configure_libhive

