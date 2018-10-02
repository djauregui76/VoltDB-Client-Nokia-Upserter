package com.nsn.benchmark.volt;

import com.nsn.volt.DataBuffer;
import com.nsn.volt.VoltDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 火墙数据入数据库操作类
 *
 * @author zhangxu
 */
public class FirewallProcessor implements Consumer<Path> {
	private static final Logger logger = LoggerFactory.getLogger(FirewallProcessor.class);
	/**
	 * 单条火墙记录长度
	 */
	private static final int FIREWALL_LENGTH = 30;
	/**
	 * 单条火墙记录中 key 长度
	 */
	private static final int FIREWALL_KEY_LENGTH = 12;
	/**
	 * value 长度
	 */
	private static final int FIREWALL_VALUE_LENGTH = 6;
	/**
	 * 时间字段长度
	 */
	private static final int FIREWALL_BUSITIME_LENGTH = 4;
	/**
	 * 无效字段长度
	 */
	private static final int FIREWALL_SKIP_LENGTH = FIREWALL_LENGTH - FIREWALL_KEY_LENGTH - FIREWALL_VALUE_LENGTH
			- FIREWALL_BUSITIME_LENGTH;

	@Override
	public void accept(Path path) {
		/**
		 * 定义批量处理
		 */
		DataBuffer<AbstractMap.SimpleEntry<Object, Object[]>> buffer = new DataBuffer<>(entries -> {
			Map<Object, Object[]> map = new HashMap<>();
			entries.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
			try {
				// 插入数据库
				Metrics.accumulator_firewall.add(VoltDB.voltDBClient.setObjects("FirewallWindow", map, false));
			} catch (Exception e) {
				logger.error("", e);
			}
		});

		// 读取文件
		try (InputStream is = Files.newInputStream(path)) {
			for (; is.available() >= FIREWALL_LENGTH;) {
				byte[] key = new byte[FIREWALL_KEY_LENGTH];
				byte[] value = new byte[FIREWALL_VALUE_LENGTH];
				byte[] busitime = new byte[FIREWALL_BUSITIME_LENGTH];
				is.read(key);
				is.read(value);
				is.read(busitime);
				is.skip(FIREWALL_SKIP_LENGTH);
				AbstractMap.SimpleEntry<Object, Object[]> entry = new AbstractMap.SimpleEntry<>(key,
						new Object[] { value, bytesToTime(busitime) });
				buffer.add(entry);
			}
			buffer.flush();
			Metrics.firewall_Gbyte.add((double) path.toFile().length() / 1024 / 1024 / 1024);
			logger.debug(MessageFormat.format("name={0} , size={1}M", path.toString(),
					String.format("%.2f", (double) path.toFile().length() / 1024 / 1024)));
		} catch (Exception e) {
			logger.error("filename = " + path.toString(), e);
		}
	}

	/**
	 * @return millisecond
	 */
	private long bytesToTime(byte[] bytes) {
		return byteArrayToLong(bytes) * 1000;
	}

	private long byteArrayToLong(byte[] value) {
		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}
		if (value.length != 4) {
			throw new IllegalArgumentException("value length must be 4");
		}
		return ((value[0] << 24) & 0xFF000000L) | ((value[1] << 16) & 0x00FF0000) | ((value[2] << 8) & 0x0000FF00)
				| ((value[3] & 0xFF));
	}
}
