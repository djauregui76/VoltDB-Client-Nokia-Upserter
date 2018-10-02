package com.nsn.voltdb.procs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * @author zhangxu
 */
public class SelectFromFirewall extends VoltProcedure {
    private final SQLStmt query = new SQLStmt(
            "SELECT T.INDEX, P.SOURCE " +
                    "FROM TEMPORARY_Firewall T LEFT JOIN Firewall P ON (T.CONDITION = P.CONDITION) " +
                    "WHERE " +
                    "    T.HOST = ?" +
                    "    AND T.THREAD_NAME = ?" +
                    "    AND T.CREATED  = TO_TIMESTAMP(MILLIS, ?);");

    public VoltTable[] run(byte[] partitionKey, String host, String threadName, long millis) {
        voltQueueSQL(query, host, threadName, millis);
        return voltExecuteSQL(true);
    }
}