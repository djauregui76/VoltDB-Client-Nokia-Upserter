package com.nsn.benchmark.volt;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.nsn.volt.DataBuffer;
import com.nsn.volt.VoltDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author zhangxu
 */
public class S1uDelayProcessor implements Consumer<Path> {
	private static final Logger logger = LoggerFactory.getLogger(S1uDelayProcessor.class);
	/**
	 * 文件输出工具
	 */
	private Consumer<String> writer;
	/**
	 * 数据文件分隔符
	 */
	private Splitter splitter = Splitter.on('|');
	/**
	 * 输出文件分隔符
	 */
	private Joiner joiner = Joiner.on('|');

	public S1uDelayProcessor(Consumer<String> writer) {
		this.writer = writer;
	}

	/**
	 * 转换相关字段到二进制 key
	 *
	 * @param privateHost
	 *            私网地址
	 * @param privatePort
	 *            私网端口
	 * @param destHost
	 *            目的地址
	 * @param destPort
	 *            目的端口
	 * @return
	 */
	private static byte[] encodeKey(String privateHost, int privatePort, String destHost, int destPort) {
		byte[][] array = new byte[4][];
		try {
			array[0] = hostToBytes(privateHost);
			array[1] = portToBytes(privatePort);
			array[2] = hostToBytes(destHost);
			array[3] = portToBytes(destPort);
			// 12 = (len(IPv4) + len(port)) * 2 = (4 + 2) * 2
			ByteBuffer buffer = ByteBuffer.allocate(12);
			buffer.put(array[0]);
			buffer.put(array[1]);
			buffer.put(array[2]);
			buffer.put(array[3]);
			return buffer.array();
		} catch (Exception e) {
			logger.error("", e);
		}
		return new byte[12];
	}

	/**
	 * 解析二进制数据内容问字符串形式
	 *
	 * @param bytes
	 *            二进制数据
	 * @return
	 * @throws UnknownHostException
	 */
	private static String[] decodeValue(byte[] bytes) throws UnknownHostException {
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		// 源IP
		byte[] foo = new byte[4];
		byteBuffer.get(foo);
		// 源端口
		byte[] bar = new byte[2];
		byteBuffer.get(bar);
		return new String[] { bytesToHost(foo), String.valueOf(bytesToPort(bar)) };
	}

	/**
	 * 转换明文 IPv4 地址到二进制
	 *
	 * @param host
	 *            地址
	 * @return
	 * @throws UnknownHostException
	 */
	private static byte[] hostToBytes(String host) throws UnknownHostException {
		return InetAddress.getByName(host).getAddress();
	}

	/**
	 * 转换端口到二进制
	 *
	 * @param port
	 *            端口
	 * @return
	 */
	private static byte[] portToBytes(int port) {
		return new byte[] { (byte) (port >> 8), (byte) (port /* >> 0 */) };
	}

	/**
	 * 二进制转换为端口
	 *
	 * @param value
	 *            二进制数据
	 * @return
	 */
	private static int bytesToPort(byte[] value) {
		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}
		if (value.length != 2) {
			throw new IllegalArgumentException("value length must be 2");
		}
		return ((value[0] << 8) & 0x0000FF00) | ((value[1] & 0xFF));// NOPMD
	}

	/**
	 * 转换二进制数据为 IPv4 地址
	 *
	 * @param bytes
	 * @return
	 * @throws UnknownHostException
	 */
	private static String bytesToHost(byte[] bytes) throws UnknownHostException {
		return InetAddress.getByAddress(bytes).getHostAddress();
	}

	@Override
	public void accept(Path path) {
		// 定义批量执行工具
		DataBuffer<AbstractMap.SimpleEntry<Object[], String[]>> buffer = new DataBuffer<>(
				entries -> VoltDB.voltDBClient.getBytes("FirewallWindow", entries, (bytes, strings) -> {
					// bytes to value
					try {
						// 将获取内容转换为明文字段
						String[] foo = Objects.isNull(bytes) ? new String[2] : decodeValue(bytes);
						// 回填到记录最前并输出到文件
						writer.accept(String.format("%s|%s|%s", foo[0], foo[1], joiner.join(strings)));
					} catch (UnknownHostException e) {
						logger.error("", e);
					}
				}));

		try (Reader r = Channels.newReader(FileChannel.open(path),
				StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPLACE), -1);
				BufferedReader reader = new BufferedReader(r);
				Stream<String> stream = reader.lines()) {
			// split
			stream.map(line -> Iterables.toArray(splitter.split(line), String.class)).forEach(strings -> {
				AbstractMap.SimpleEntry<Object[], String[]> entry = new AbstractMap.SimpleEntry<>(new Object[] {
						encodeKey(strings[26], Integer.valueOf(strings[28]), strings[30], Integer.valueOf(strings[32])),
						strings[19] }, strings);
				buffer.add(entry);
			});
			buffer.flush();
			Metrics.file_s1udelay_dealcount.getAndIncrement();
			Metrics.s1udelay_Gbyte.add((double) path.toFile().length() / 1024 / 1024 / 1024);
			logger.debug(MessageFormat.format("name={0} , size={1}M", path.toString(),
					String.format("%.2f", (double) path.toFile().length() / 1024 / 1024)));
		} catch (Exception e) {
			logger.error("filename = " + path.toString(), e);
		}
	}

}
