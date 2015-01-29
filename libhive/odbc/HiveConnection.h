/**************************************************************************//**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 *
 * @file HiveConnection.h
 * @brief Provides the HiveConnection struct
 *
 *****************************************************************************/


#ifndef __hive_connection_h__
#define __hive_connection_h__

#include "ThriftHive.h"
#include "TCLIService.h"
#include "HiveConnection.h"
#include <boost/shared_ptr.hpp>

using namespace boost;
using namespace apache::thrift::transport;
using namespace apache::hive::service::cli::thrift;
using namespace Apache::Hadoop::Hive;

/*************************************************************************************************
 * HiveConnection Class Definition
 ************************************************************************************************/

class HiveConnection {
private:
  shared_ptr<TTransport>        m_pTransport;
  shared_ptr<ThriftHiveClient>  m_pClient1;
  shared_ptr<TCLIServiceClient> m_pClient2;
  TSessionHandle                *hive_session_handle;
  int                           version;
public:
  HiveConnection() {
  }
  HiveConnection(shared_ptr<ThriftHiveClient> c, shared_ptr<TTransport> t) {
    m_pClient1 = c;
    m_pTransport = t;
  }
  HiveConnection(shared_ptr<TCLIServiceClient> c, shared_ptr<TTransport> t) { 
    m_pClient2 = c;
    m_pTransport = t;
  }
  virtual ~HiveConnection() { /* Nothing to delete */ }
  
  virtual inline void setVersion(int v) { version = v; }
  virtual inline int getVersion() { return version; }
  
  virtual inline shared_ptr<ThriftHiveClient> getClient1() { return m_pClient1; }
  virtual inline void setClient1(shared_ptr<ThriftHiveClient> c) { m_pClient1 = c; }

  virtual inline shared_ptr<TCLIServiceClient> getClient2() { return m_pClient2; }
  virtual inline void setClient2(shared_ptr<TCLIServiceClient> c) { m_pClient2 = c; }

  virtual inline shared_ptr<TTransport> getTransport() { return m_pTransport; }
  virtual inline void setTransport(shared_ptr<TTransport> t) { m_pTransport = t; }
  
  virtual inline void setSessionHandle(TSessionHandle *s) { hive_session_handle = s; }
  virtual inline TSessionHandle *getSessionHandle() { return hive_session_handle; }

  virtual HiveReturn closeConnection() {
    TCloseSessionReq  *req = new TCloseSessionReq();
    TCloseSessionResp *res = new TCloseSessionResp();
    try {
      if (version == 1) {
        req->__set_sessionHandle(*(hive_session_handle));
        m_pClient2->CloseSession(*res, *req);
        m_pTransport->close();
      }
      m_pTransport->close();
    } catch (...) {
      /* Ignore the exception, we just want to clean up everything...  */
    }
    return HIVE_SUCCESS;
  }
};


#endif // __hive_connection_h__
