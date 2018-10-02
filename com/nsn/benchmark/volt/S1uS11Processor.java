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
public class S1uS11Processor implements Consumer<Path> {
	private static final Logger logger = LoggerFactory.getLogger(S1uS11Processor.class);
	private Consumer<String> writer;
	private Splitter splitter = Splitter.on('|');
	private Joiner joiner = Joiner.on('|');
	private DataBuffer<AbstractMap.SimpleEntry<Object[], String[]>> buffer = new DataBuffer<>(
			entries -> VoltDB.voltDBClient.getStrings("Imsi", entries, (values, strings) -> {
				// bytes to value
				if (!Objects.isNull(values[0])) {
					strings[6] = values[0];
					strings[7] = values[1];
				}
				writer.accept(joiner.join(strings));
			}));

	public S1uS11Processor(Consumer<String> writer) {
		this.writer = writer;
	}

	@Override
	public void accept(Path path) {
		try (Reader r = Channels.newReader(FileChannel.open(path),
				StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPLACE), -1);
				BufferedReader reader = new BufferedReader(r);
				Stream<String> stream = reader.lines()) {
			// split
			stream.map(line -> Iterables.toArray(splitter.split(line), String.class)).forEach(strings -> {
				AbstractMap.SimpleEntry<Object[], String[]> entry =
						// 5, 6, 7 下表分别对应字段 imsi, imei, msisdn，这里使用 imsi 作为查询 key
						new AbstractMap.SimpleEntry<>(new Object[] { strings[5] }, strings);
				buffer.add(entry);
			});
			buffer.flush();
			if (path.toString().contains("s11")) {
				Metrics.s11_Gbyte.add((double) path.toFile().length() / 1024 / 1024 / 1024);
			} else {
				Metrics.s1u_Gbyte.add((double) path.toFile().length() / 1024 / 1024 / 1024);
			}
			logger.debug(MessageFormat.format("name={0} , size={1}M", path.toString(),
					String.format("%.2f", (double) path.toFile().length() / 1024 / 1024)));
		} catch (Exception e) {
			logger.error("filename = " + path.toString(), e);
		}
	}

}
