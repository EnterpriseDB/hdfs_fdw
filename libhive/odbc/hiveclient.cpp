/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <assert.h>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>

#include "ThriftHive.h"
#include <protocol/TBinaryProtocol.h>
#include <transport/TTransportUtils.h>
#include <transport/TTransport.h>
#include <transport/TSocket.h>

#include "hiveclient.h"
#include "hiveclienthelper.h"
#include "HiveColumnDesc.h"
#include "HiveConnection.h"
#include "HiveResultSet.h"
#include "HiveRowSet.h"


using namespace std;
using namespace boost;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;


/*****************************************************************
 * Global Hive Client Functions (usable as C callback functions)
 *****************************************************************/

HiveConnection* DBOpenConnection(const char* database, const char* host, int port, int framed,
                                 HIVE_SERVER_TYPE v, int coonect_timeout, int receive_timeout, char* err_buf, size_t err_buf_len) {
/* TODO: Add in database selection when Hive supports this feature. */
  shared_ptr<TSocket>           socket(new TSocket(host, port));
  shared_ptr<TTransport>        transport;
  HiveConnection                *conn = NULL;
  shared_ptr<TCLIServiceClient> hs2_client;
  shared_ptr<ThriftHiveClient>  hs1_client;	
  TOpenSessionReq               *req = NULL;
  TOpenSessionResp              *res = NULL;

  /* Set the timeout if timeout is greater than 0 */
  if (coonect_timeout > 0)
    socket->setConnTimeout(coonect_timeout);

  /* Set the timeout if timeout is greater than 0 */
  if (receive_timeout > 0)
    socket->setRecvTimeout(receive_timeout);

  conn = new HiveConnection(); /* Will set client and tranport later */

  if (framed) {
    shared_ptr<TFramedTransport> framedSocket(new TFramedTransport(socket));
    transport = framedSocket;
  } else {
    shared_ptr<TBufferedTransport> bufferedSocket(new TBufferedTransport(socket));
    transport = bufferedSocket;
  }

  shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
  req = new TOpenSessionReq();

  req->__set_client_protocol((TProtocolVersion::type)0);

  try {
    switch (v) {
      case HIVE_SERVER1: {
        transport->open();
	shared_ptr<ThriftHiveClient> c (new ThriftHiveClient(protocol));
        conn->setClient1(c);
	conn->setTransport(transport);
        break;
      }
      case HIVE_SERVER2: {
        transport->open();
        res = new TOpenSessionResp();
        shared_ptr<TCLIServiceClient> c (new TCLIServiceClient(protocol));
	conn->setClient2(c);
        conn->getClient2()->OpenSession(*res, *req);
        conn->setTransport(transport);
        conn->setSessionHandle(&res->sessionHandle);
        break;
      }
      default:
        RETURN_FAILURE(__FUNCTION__,
                       "Not support version of Hive Server specified.", err_buf, err_buf_len, NULL);
      break;
    }
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err_buf, err_buf_len, NULL);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
                   "Unable to connect to Hive server.", err_buf, err_buf_len, NULL);
  }
  conn->setVersion(v);
  return conn; 
}
  
HiveReturn DBCloseConnection(HiveConnection* connection, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(connection == NULL, __FUNCTION__,
		"Hive connection cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(connection->getTransport() == NULL, __FUNCTION__,
                   "Hive connection transport cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);

  HiveReturn r = connection->closeConnection();
  delete connection;
  return r;
}

HiveReturn DBExecute(HiveConnection* connection, const char* query, HiveResultSet** resultset_ptr,
                     int max_buf_rows, char* err_buf, size_t err_buf_len) {

  RETURN_ON_ASSERT(connection == NULL, __FUNCTION__,
                   "Hive connection cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);

  RETURN_ON_ASSERT(connection->getVersion() == HIVE_SERVER1 && connection->getClient1() == NULL, __FUNCTION__,
                   "Hive connection client cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  
  RETURN_ON_ASSERT(connection->getVersion() == HIVE_SERVER2 && connection->getClient2() == NULL, __FUNCTION__,
                   "Hive connection client cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
    
  RETURN_ON_ASSERT(query == NULL, __FUNCTION__,
                   "Query string cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  
  RETURN_ON_ASSERT(resultset_ptr == NULL, __FUNCTION__,
                   "Result Pointer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);

  HiveQueryResultSet* query_resultset = NULL;
  
  string query_str(query);

  /* TODO: this may not need to happen if Hive allows for multiple successive queries in
   * one execute statement (and permits a terminating semicolon).
   */
  
  /* Strip off a query's terminating semicolon if it exists */
  trim(query_str); /* Trim white space from string to check if last character is semicolon */
  if (query_str.length() > 0 && query_str[query_str.length() - 1] == ';') {
    query_str.erase(query_str.length() - 1);
  }

  /* Pass the query onto the Hive server for execution */
  /* Query execution is kept separate from the resultset b/c results may not always be needed (i.e. DML) */
  try {
    if(connection->getVersion() == HIVE_SERVER1) {
        connection->getClient1()->execute(query_str);
        query_resultset = new HiveQueryResultSetHS1(max_buf_rows);
        query_resultset->initialize(connection, query_str, err_buf, err_buf_len);
      } else {
        query_resultset = new HiveQueryResultSetHS2(1000);
        query_resultset->initialize(connection, query_str, err_buf, err_buf_len);
      }
    } catch (Apache::Hadoop::Hive::HiveServerException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
    } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
    "Unknown Hive query execution error.", err_buf, err_buf_len, HIVE_ERROR);
  }
  *resultset_ptr = query_resultset;
  return HIVE_SUCCESS;
}

HiveReturn DBColumns(HiveConnection* connection, int(*fpHiveToSQLType)(HiveType),
                     const char* tbl_search_pattern, const char* col_search_pattern,
                     HiveResultSet** resultset_ptr, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset_ptr == NULL, __FUNCTION__,
                   "Resultset pointer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);

  HiveColumnsResultSet* columns_resultset = new HiveColumnsResultSet(fpHiveToSQLType);
  *resultset_ptr = columns_resultset; /* Store into generic HiveResultSet pointer */
  return columns_resultset->initialize(connection, tbl_search_pattern, col_search_pattern, err_buf,
                                       err_buf_len);
}

HiveReturn DBCloseResultSet(HiveResultSet* resultset, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  delete resultset;
  return HIVE_SUCCESS;
}

HiveReturn DBFetch(HiveResultSet* resultset, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->fetchNext(err_buf, err_buf_len);
}

HiveReturn DBHasResults(HiveResultSet* resultset, int* has_results, char* err_buf,
                        size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->hasResults(has_results, err_buf, err_buf_len);
}

HiveReturn DBGetColumnCount(HiveResultSet* resultset, size_t* col_count, char* err_buf,
                            size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getColumnCount(col_count, err_buf, err_buf_len);
}

HiveReturn DBCreateColumnDesc(HiveResultSet* resultset, size_t column_idx,
                              HiveColumnDesc** column_desc_ptr, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->createColumnDesc(column_idx, column_desc_ptr, err_buf, err_buf_len);
}

HiveReturn DBGetFieldDataLen(HiveResultSet* resultset, size_t column_idx, size_t* col_len,
                             char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
    
  return resultset->getFieldDataLen(column_idx, col_len, err_buf, err_buf_len);
}

HiveReturn DBGetFieldAsCString(HiveResultSet* resultset, size_t column_idx, char* buffer,
                               size_t buffer_len, size_t* data_byte_size, int* is_null_value,
                               char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getFieldAsCString(column_idx, buffer, buffer_len, data_byte_size,
                                                  is_null_value, err_buf, err_buf_len);
}

HiveReturn DBGetFieldAsDouble(HiveResultSet* resultset, size_t column_idx, double* buffer,
                              int* is_null_value, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getRowSet().getFieldAsDouble(column_idx, buffer, is_null_value, err_buf,
                                                 err_buf_len);
}

HiveReturn DBGetFieldAsInt(HiveResultSet* resultset, size_t column_idx, int* buffer,
                           int* is_null_value, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getRowSet().getFieldAsInt(column_idx, buffer, is_null_value, err_buf,
                                              err_buf_len);
}

HiveReturn DBGetFieldAsLong(HiveResultSet* resultset, size_t column_idx, long* buffer,
                            int* is_null_value, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getRowSet().getFieldAsLong(column_idx, buffer, is_null_value, err_buf,
                                               err_buf_len);
}

HiveReturn DBGetFieldAsULong(HiveResultSet* resultset, size_t column_idx, unsigned long* buffer,
                             int* is_null_value, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getRowSet().getFieldAsULong(column_idx, buffer, is_null_value, err_buf,
                                                err_buf_len);
}

HiveReturn DBGetFieldAsI64(HiveResultSet* resultset, size_t column_idx, int64_t* buffer,
                           int* is_null_value, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getRowSet().getFieldAsI64(column_idx, buffer, is_null_value, err_buf,
                                              err_buf_len);
}

HiveReturn DBGetFieldAsI64U(HiveResultSet* resultset, size_t column_idx, uint64_t* buffer,
                            int* is_null_value, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(resultset == NULL, __FUNCTION__,
                   "Hive resultset cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  return resultset->getRowSet().getFieldAsI64U(column_idx, buffer, is_null_value, err_buf,
                                               err_buf_len);
}

HiveReturn DBCloseColumnDesc(HiveColumnDesc* column_desc, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(column_desc == NULL, __FUNCTION__,
                   "Hive column descriptor cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  delete column_desc;
  return HIVE_SUCCESS;
}

/* Forego the error message handling in these accessor functions b/c of trivial implementations */
void DBGetColumnName(HiveColumnDesc* column_desc, char* buffer, size_t buffer_len) {
  assert(column_desc != NULL);
  assert(buffer != NULL);
  column_desc->getColumnName(buffer, buffer_len);
}

void DBGetColumnType(HiveColumnDesc* column_desc, char* buffer, size_t buffer_len) {
  assert(column_desc != NULL);
  assert(buffer != NULL);
  column_desc->getColumnType(buffer, buffer_len);
}

HiveType DBGetHiveType(HiveColumnDesc* column_desc) {
  assert(column_desc != NULL);
  return column_desc->getHiveType();
}

int DBGetIsNullable(HiveColumnDesc* column_desc) {
  assert(column_desc != NULL);
  return column_desc->getIsNullable();
}

int DBGetIsCaseSensitive(HiveColumnDesc* column_desc) {
  assert(column_desc != NULL);
  return column_desc->getIsCaseSensitive();
}

size_t DBGetMaxDisplaySize(HiveColumnDesc* column_desc) {
  assert(column_desc != NULL);
  return column_desc->getMaxDisplaySize();
}

size_t DBGetFieldByteSize(HiveColumnDesc* column_desc) {
  assert(column_desc != NULL);
  return column_desc->getFieldByteSize();
}

