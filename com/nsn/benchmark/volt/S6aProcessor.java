package com.nsn.benchmark.volt;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.nsn.volt.DataBuffer;
import com.nsn.volt.VoltDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author zhangxu
 */
public class S6aProcessor implements Consumer<Path> {
	private static final Logger logger = LoggerFactory.getLogger(S6aProcessor.class);
	private Splitter splitter = Splitter.on('|');
	private DataBuffer<String[]> buffer = new DataBuffer<>(entries -> {
		Map<Object, Object[]> map = new HashMap<>();
		entries.forEach(strings -> map.put(strings[0], new String[] { strings[1], strings[2] }));
		try {
			Metrics.accumulator_s6a.add(VoltDB.voltDBClient.setObjects("Imsi", map, true));
		} catch (Exception e) {
			logger.error("", e);
		}
	});

	@Override
	public void accept(Path path) {
		try (Reader r = Channels.newReader(FileChannel.open(path),
				StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPLACE), -1);
				BufferedReader reader = new BufferedReader(r);
				Stream<String> stream = reader.lines()) {
			// split
			stream.map(line -> Iterables.toArray(splitter.split(line), String.class))
					// 5, 6, 7 下表分别对应字段 imsi, imei, msisdn
					.forEach(strings -> buffer.add(new String[] { strings[5], strings[6], strings[7] }));
			buffer.flush();
			Metrics.s6a_Gbyte.add((double) path.toFile().length() / 1024 / 1024 / 1024);
			logger.debug(MessageFormat.format("name={0} , size={1}M", path.toString(),
					String.format("%.2f", (double) path.toFile().length() / 1024 / 1024)));
		} catch (Exception e) {
			logger.error("filename = " + path.toString(), e);
		}
	}
}
