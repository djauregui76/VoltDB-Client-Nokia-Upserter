package com.nsn.volt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

import com.google.common.base.Splitter;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Random;

/**
 * @author zhangxu
 */
public class VoltDBPanel implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(VoltDBPanel.class);
	private static String HOST;

	/**
	 * 获取本地网络地址
	 */
	static {
		try {
			HOST = Inet4Address.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private String selectProcedurePrefix;
	private String deleteProcedurePrefix;
	private String temporaryPrefix;
	/**
	 * 批量执行大小
	 */
	private int batchSize;
	private Client[] clients = new Client[2];
	private AtomicInteger next = new AtomicInteger(0);

	private Client getClient() {
		return clients[next.getAndIncrement() % 2];
	}

	private String connectString;
	private byte[][] partitionKeys;

	private VoltDBPanel(Builder builder) {
		this.connectString = builder.connectString;
		this.batchSize = builder.batchSize;
		this.selectProcedurePrefix = builder.selectProcedurePrefix;
		this.deleteProcedurePrefix = builder.deleteProcedurePrefix;
		this.temporaryPrefix = builder.temporaryPrefix;
	}

	/**
	 * 连接 VoltDB 服务器
	 *
	 * @throws Exception
	 */
	private void connect() throws Exception {
		for (int i = 0; i < clients.length; i++) {
			clients[i] = ClientFactory.createClient();
			Iterator<String> iter = Splitter.on(',').split(this.connectString).iterator();
			while (iter.hasNext()) {
				clients[i].createConnection(iter.next());
			}
		}
		partitionKeys = getPartitionKeys();
	}

	/**
	 * 获取分区信息
	 *
	 * @return
	 * @throws Exception
	 */
	private byte[][] getPartitionKeys() throws Exception {
		VoltTable table = getClient().callProcedure("@GetPartitionKeys", "VARBINARY").getResults()[0];
		byte[][] keys = new byte[table.getRowCount()][];
		while (table.advanceRow()) {
			keys[table.getActiveRowIndex()] = table.getVarbinary("PARTITION_KEY");
		}
		return keys;
	}

	/**
	 * 调用存储过程
	 *
	 * @param procedureName
	 *            存储过程名
	 * @param parameters
	 *            参数
	 * @param callback
	 *            回调函数
	 * @throws IOException
	 */
	private void callProcedure(String procedureName, Object[] parameters, ProcedureCallback callback)
			throws IOException {
		Object[] rewrite = new Object[parameters.length + 1];
		System.arraycopy(parameters, 0, rewrite, 1, parameters.length);
		for (byte[] key : partitionKeys) {
			int index = 0;
			do {
				if (index++ > 1) {
					logger.warn("Retry procedure '{}' {} times", procedureName, index);
				}
				rewrite[0] = key;
			} while (!getClient().callProcedure(callback, procedureName, rewrite));
		}
	}

	/**
	 * 异步调用存储过程获取查询内容
	 *
	 * @param tableName
	 *            表名
	 * @param kList
	 *            字段
	 * @param callback
	 *            回调
	 */
	private void get(String tableName, List<Object[]> kList, ProcedureCallback callback) {
		// 获取必要的运行信息
		String threadName = Thread.currentThread().getName();
		long currentTime = System.currentTimeMillis() * 1000 + new Random().nextInt(1000);
		// 整理数据格式
		List<Object[]> rList = IntStream.range(0, kList.size()).mapToObj(value -> {
			int length = kList.get(value).length;
			Object[] parameters = new Object[length + 4];
			System.arraycopy(kList.get(value), 0, parameters, 0, length);
			parameters[length] = value;
			parameters[length + 1] = HOST;
			parameters[length + 2] = threadName;
			parameters[length + 3] = currentTime;
			return parameters;
		}).collect(Collectors.toList());
		try {
			// 插入到临时表中
			batchSet(temporaryPrefix + tableName, false, rList);
			// 调用存储过程
			callProcedure(selectProcedurePrefix + tableName, new Object[] { HOST, threadName, currentTime }, callback);
			// 由于修改为异步调用，可能会发生在异步调用完整执行之前删除临时表数据的情况。这里关闭掉 client 端发起的数据清除请求，转由临时表增加 ttl
			// 字段进行数据的清空操作
			// client.callProcedure(deleteProcedurePrefix + tableName, HOST, threadName,
			// currentTime / 1000);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	/**
	 * 异步调用存储过程
	 *
	 * @param tableName
	 *            表名
	 * @param entryList
	 *            数据
	 * @param consumer
	 *            回调
	 * @param function
	 *            字段提取
	 * @param <V>
	 *            数据库返回格式
	 * @param <T>
	 *            传入异步函数的数据
	 */
	private <V, T> void get(String tableName, List<AbstractMap.SimpleEntry<Object[], T>> entryList,
			BiConsumer<V, T> consumer, Function<VoltTable, V> function) {
		// 整理数据结构
		List<Object[]> kList = entryList.stream().map(AbstractMap.SimpleEntry::getKey).collect(Collectors.toList());
		List<T> vList = entryList.stream().map(AbstractMap.SimpleEntry::getValue).collect(Collectors.toList());
		get(tableName, kList, response -> {
			if (response.getStatus() == ClientResponse.SUCCESS) {
				for (VoltTable table : response.getResults()) {
					while (table.advanceRow()) {
						Integer index = (Integer) table.get(0, VoltType.INTEGER);
						consumer.accept(function.apply(table), vList.get(index));
					}
				}
			} else {
				vList.forEach(t -> consumer.accept(null, t));
				logger.error("Error response: ({}, {})", response.getStatusString(), response.getAppStatusString());
			}
		});
	}

	public <T> void getBytes(String tableName, List<AbstractMap.SimpleEntry<Object[], T>> kList,
			BiConsumer<byte[], T> consumer) {
		get(tableName, kList, consumer, voltTable -> voltTable.getVarbinary(1));
	}

	public <T> void getString(String tableName, List<AbstractMap.SimpleEntry<Object[], T>> kList,
			BiConsumer<String, T> consumer) {
		get(tableName, kList, consumer, voltTable -> voltTable.getString(1));
	}

	public <T> void getStrings(String tableName, List<AbstractMap.SimpleEntry<Object[], T>> kList,
			BiConsumer<String[], T> consumer) {
		get(tableName, kList, consumer, voltTable -> {
			int vLength = voltTable.getColumnCount() - 1;
			String[] values = new String[vLength];
			for (int i = 1; i <= vLength; i++) {
				values[i - 1] = voltTable.getString(i);
			}
			return values;
		});
	}

	/**
	 * 批量数据插入
	 *
	 * @param tableName
	 *            表名
	 * @param upsert
	 *            插入方式
	 * @param list
	 *            插入字段
	 * @return 插入条数
	 * @throws Exception
	 */
	private long batchSet(String tableName, boolean upsert, List<Object[]> list) throws Exception {
		VoltBulkLoader bulkLoader = getClient().getNewBulkLoader(tableName, batchSize, upsert,
				new SessionBulkLoaderFailureCallback());
		for (Object[] objects : list) {
			bulkLoader.insertRow(objects[0], objects);
		}
		bulkLoader.flush();
		bulkLoader.drain();
		bulkLoader.close();
		return bulkLoader.getCompletedRowCount();
	}

	public long set(String tableName, Map<byte[], byte[]> data) throws Exception {
		long timestamp = System.currentTimeMillis() * 1000 + new Random().nextInt(1000);
		return batchSet(tableName, true, data.keySet().stream().map(k -> new Object[] { k, data.get(k), timestamp })
				.collect(Collectors.toList()));
	}

	public long setString(String tableName, Map<String, String> data) throws Exception {
		long timestamp = System.currentTimeMillis() * 1000 + new Random().nextInt(1000);
		return batchSet(tableName, true, data.keySet().stream().map(k -> new Object[] { k, data.get(k), timestamp })
				.collect(Collectors.toList()));
	}

	public long setObjects(String tableName, Map<Object, Object[]> data, boolean upsert) throws Exception {
		long timestamp = System.currentTimeMillis() * 1000 + new Random().nextInt(1000);
		return batchSet(tableName, upsert, data.keySet().stream().map(k -> {
			int length = data.get(k).length;
			Object[] params = new Object[length + 2];
			params[0] = k;
			System.arraycopy(data.get(k), 0, params, 1, length);
			params[length + 1] = timestamp;
			return params;
		}).collect(Collectors.toList()));
	}

	@Override
	public void close() throws Exception {
		for (int i = 0; i < clients.length; i++) {
			clients[i].close();
		}
	}

	public static class SessionBulkLoaderFailureCallback implements BulkLoaderFailureCallBack {
		@Override
		public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
			if (response.getStatus() != ClientResponse.SUCCESS) {
				logger.error("Error response: ({}, {})", response.getStatusString(), response.getAppStatusString());
			}
		}
	}

	public static class Builder {
		private Map<String, VoltDBPanel> panelCache = new ConcurrentHashMap<>();
		private String connectString;
		private int batchSize = 6500;

		private String selectProcedurePrefix = "SelectFrom";
		private String deleteProcedurePrefix = "DeleteFrom";
		private String temporaryPrefix = "TEMPORARY_";

		public Builder(String connectString) {
			this.connectString = connectString;
		}

		public VoltDBPanel.Builder batchSize(int size) {
			this.batchSize = size;
			return this;
		}

		public Builder selectProcedurePrefix(String prefix) {
			this.selectProcedurePrefix = prefix;
			return this;
		}

		public Builder deleteProcedurePrefix(String prefix) {
			this.deleteProcedurePrefix = prefix;
			return this;
		}

		public Builder temporaryPrefix(String prefix) {
			this.temporaryPrefix = prefix;
			return this;
		}

		public VoltDBPanel build() throws Exception {
			VoltDBPanel panel = panelCache.get(connectString);
			if (Objects.isNull(panel)) {
				panel = new VoltDBPanel(this);
				panel.connect();
				panelCache.put(connectString, panel);
			}
			return panel;
		}
	}
}
