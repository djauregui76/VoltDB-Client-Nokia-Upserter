package com.nsn.voltdb.procs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * @author zhangxu
 */
public class SelectFromImsi extends VoltProcedure {
    private final SQLStmt query = new SQLStmt(
            "SELECT T.INDEX, P.IMEI ,P.MSISDN  " +
                    "FROM TEMPORARY_Imsi T LEFT JOIN Imsi P ON (T.IMSI = P.IMSI) " +
                    "WHERE " +
                    "    T.HOST = ?" +
                    "    AND T.THREAD_NAME = ?" +
                    "    AND T.CREATED  = TO_TIMESTAMP(MILLIS, ?);");

    public VoltTable[] run(String partitionKey, String host, String threadName, long millis) {
        voltQueueSQL(query, host, threadName, millis);
        return voltExecuteSQL(true);
    }
}