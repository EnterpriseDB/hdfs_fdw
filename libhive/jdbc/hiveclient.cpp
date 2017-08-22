/*-------------------------------------------------------------------------
 *
 * hiveclient.cpp
 * 		Wrapper functions to call functions in the Java class HiveJdbcClient
 * 		from C/C++
 * 
 * Copyright (c) 2017, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hiveclient.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <assert.h>
#include <iostream>
#include <string.h>


#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>

#include "hiveclient.h"

using namespace std;

static JavaVM *g_jvm = NULL;
static JNIEnv *g_jni = NULL;
static jclass g_clsMsgBuf = NULL;
static jobject g_objMsgBuf = NULL;
static jobject g_objValBuf = NULL;
static jclass g_clsJdbcClient = NULL;
static jobject g_objJdbcClient = NULL;
static jmethodID g_getVal = NULL;
static jmethodID g_resetVal = NULL;
static jmethodID g_DBOpenConnection = NULL;
static jmethodID g_DBCloseConnection = NULL;
static jmethodID g_DBCloseAllConnections = NULL;
static jmethodID g_DBExecute = NULL;
static jmethodID g_DBExecuteUtility = NULL;
static jmethodID g_DBCloseResultSet = NULL;
static jmethodID g_DBFetch = NULL;
static jmethodID g_DBGetColumnCount = NULL;
static jmethodID g_DBGetFieldAsCString = NULL;

typedef jint ((*_JNI_CreateJavaVM_PTR)(JavaVM **p_vm, JNIEnv **p_env, void *vm_args));
_JNI_CreateJavaVM_PTR _JNI_CreateJavaVM;
void* hdfs_dll_handle = NULL;

int Initialize()
{
	jint            ver;
	jint            rc;
	JavaVMInitArgs  vm_args;
	JavaVMOption*   options;
	jmethodID       consMsgBuf;
	jmethodID       consJdbcClient;
	int             len;
    char            *libjvm;

    len = strlen(g_jvmpath);
    libjvm = new char[len + 15];
    sprintf(libjvm, "%s/%s", g_jvmpath, "libjvm.so");

    hdfs_dll_handle = dlopen(libjvm, RTLD_LAZY);
    if(hdfs_dll_handle == NULL)
    {
        return -1;
    }
    delete libjvm;

    _JNI_CreateJavaVM = (_JNI_CreateJavaVM_PTR)dlsym(hdfs_dll_handle, "JNI_CreateJavaVM");

    options = new JavaVMOption[1];

	len = strlen(g_classpath) + 100;

	options[0].optionString = new char[len];
	strcpy(options[0].optionString, "-Djava.class.path=");
	strcat(options[0].optionString, g_classpath);

	vm_args.version = JNI_VERSION_1_6;
	vm_args.nOptions = 1;
	vm_args.options = options;
	vm_args.ignoreUnrecognized = false;

    rc = _JNI_CreateJavaVM(&g_jvm, &g_jni, &vm_args);

    delete[] options[0].optionString;
	delete options;
	if (rc != JNI_OK)
	{
		return(-1);
	}
	ver = g_jni->GetVersion();

	g_clsMsgBuf = g_jni->FindClass("MsgBuf");
	if (g_clsMsgBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-2);
	}

	g_clsJdbcClient = g_jni->FindClass("HiveJdbcClient");
	if (g_clsJdbcClient == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-3);
	}

	consMsgBuf = g_jni->GetMethodID(g_clsMsgBuf, "<init>", "(Ljava/lang/String;)V");
	if (consMsgBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-4);
	}

	consJdbcClient = g_jni->GetMethodID(g_clsJdbcClient, "<init>", "()V");
	if (consJdbcClient == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-5);
	}

	g_objMsgBuf = g_jni->NewObject(g_clsMsgBuf, consMsgBuf, g_jni->NewStringUTF(""));
	if (g_objMsgBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-6);
	}

	g_objValBuf = g_jni->NewObject(g_clsMsgBuf, consMsgBuf, g_jni->NewStringUTF(""));
	if (g_objValBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-7);
	}

	g_objJdbcClient = g_jni->NewObject(g_clsJdbcClient, consJdbcClient);
	if (g_objJdbcClient == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-9);
	}

	g_getVal = g_jni->GetMethodID(g_clsMsgBuf, "getVal", "()Ljava/lang/String;");
	if (g_getVal == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-10);
	}

	g_resetVal = g_jni->GetMethodID(g_clsMsgBuf, "resetVal", "()V");
	if (g_resetVal == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-11);
	}

	g_DBOpenConnection = g_jni->GetMethodID(g_clsJdbcClient, "DBOpenConnection",
				"(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;IIILMsgBuf;)I");
	if (g_DBOpenConnection == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-12);
	}

	g_DBCloseConnection = g_jni->GetMethodID(g_clsJdbcClient, "DBCloseConnection","(I)I");
	if (g_DBCloseConnection == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-13);
	}

	g_DBCloseAllConnections = g_jni->GetMethodID(g_clsJdbcClient, "DBCloseAllConnections","()I");
	if (g_DBCloseAllConnections == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-14);
	}

	g_DBExecute = g_jni->GetMethodID(g_clsJdbcClient, "DBExecute", "(ILjava/lang/String;ILMsgBuf;)I");
	if (g_DBExecute == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-15);
	}

	g_DBExecuteUtility = g_jni->GetMethodID(g_clsJdbcClient, "DBExecuteUtility", "(ILjava/lang/String;LMsgBuf;)I");
	if (g_DBExecuteUtility == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-16);
	}

	g_DBCloseResultSet = g_jni->GetMethodID(g_clsJdbcClient, "DBCloseResultSet", "(ILMsgBuf;)I");
	if (g_DBCloseResultSet == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-17);
	}

	g_DBFetch = g_jni->GetMethodID(g_clsJdbcClient, "DBFetch", "(ILMsgBuf;)I");
	if (g_DBFetch == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-18);
	}

	g_DBGetColumnCount = g_jni->GetMethodID(g_clsJdbcClient, "DBGetColumnCount", "(ILMsgBuf;)I");
	if (g_DBGetColumnCount == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-19);
	}

	g_DBGetFieldAsCString = g_jni->GetMethodID(g_clsJdbcClient, "DBGetFieldAsCString", "(IILMsgBuf;LMsgBuf;)I");
	if (g_DBGetFieldAsCString == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-20);
	}

	return(ver);
}

int Destroy()
{
    dlclose(hdfs_dll_handle);
	if (g_jvm != NULL)
		g_jvm->DestroyJavaVM();
	return(0);
}

int DBOpenConnection(char *host, int port, char *databaseName,
					char *username, char *password,
					int connectTimeout, int receiveTimeout,
					AUTH_TYPE auth_type, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_resetVal == NULL ||
		g_DBOpenConnection == NULL || g_objMsgBuf == NULL ||
		g_getVal == NULL)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBOpenConnection,
							g_jni->NewStringUTF(host),
							port,
							g_jni->NewStringUTF(databaseName),
							g_jni->NewStringUTF(username),
							g_jni->NewStringUTF(password),
							connectTimeout,
							receiveTimeout,
							auth_type,
							g_objMsgBuf);

	rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
	*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);

	return rc;
}

int DBCloseConnection(int con_index)
{
	if (g_jni == NULL || g_objJdbcClient == NULL ||
		g_DBCloseConnection == NULL || con_index < 0)
		return(-10);

	return g_jni->CallIntMethod(g_objJdbcClient, g_DBCloseConnection, con_index);
}

int DBCloseAllConnections()
{
	if (g_jni == NULL || g_objJdbcClient == NULL ||
		g_DBCloseAllConnections == NULL)
		return(-10);

	return g_jni->CallIntMethod(g_objJdbcClient, g_DBCloseAllConnections);
}

int DBExecute(int con_index, const char* query, int maxRows, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBExecute == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		query == NULL || con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBExecute,
							con_index,
							g_jni->NewStringUTF(query),
							maxRows,
							g_objMsgBuf);
	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBExecuteUtility(int con_index, const char* query, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBExecuteUtility == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		query == NULL || con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBExecuteUtility,
							con_index,
							g_jni->NewStringUTF(query),
							g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBCloseResultSet(int con_index, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBCloseResultSet == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBCloseResultSet, con_index, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBFetch(int con_index, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBFetch == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBFetch, con_index, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBGetColumnCount(int con_index, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBGetColumnCount == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBGetColumnCount, con_index, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBGetFieldAsCString(int con_index, int columnIdx, char **buffer, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBGetFieldAsCString == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		g_objValBuf == NULL || con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	g_jni->CallVoidMethod(g_objValBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBGetFieldAsCString,
							con_index, columnIdx, g_objValBuf, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
		return(rc);
	}

	rv = (jstring)g_jni->CallObjectMethod(g_objValBuf, g_getVal);
	*buffer = (char *)g_jni->GetStringUTFChars(rv, &isCopy);

	return(strlen(*buffer));
}
