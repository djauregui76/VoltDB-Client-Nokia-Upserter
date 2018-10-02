package com.nsn.voltdb.procs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class SelectFromFirewallWindow extends VoltProcedure {
	private final SQLStmt query = new SQLStmt("SELECT distinct INDEX, SOURCE FROM ( "
			+ "SELECT T.INDEX, P.SOURCE, RANK() OVER ( PARTITION BY T.INDEX ORDER BY ABS(T.BUSITIME - P.BUSITIME),SOURCE desc) AS ROWNUM "
			+ "FROM TEMPORARY_FIREWALLWINDOW AS T LEFT OUTER JOIN FIREWALLWINDOW AS P ON (T.CONDITION = P.CONDITION) "
			+ "WHERE T.HOST = ? AND T.THREAD_NAME = ? AND T.CREATED = TO_TIMESTAMP (MILLIS, ?)) AS W "
			+ "WHERE ROWNUM = 1 ;");
	public VoltTable[] run(byte[] partitionKey, String host, String threadName, long millis) {
		voltQueueSQL(query, host, threadName, millis);
		return voltExecuteSQL(true);
	}
}
