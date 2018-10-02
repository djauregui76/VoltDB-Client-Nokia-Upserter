package com.nsn.benchmark.volt;

import com.nsn.iosystem.core.IOSystem;

import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LandData implements Runnable {
	private ArrayBlockingQueue<String> q;
	private String key;
	private IOSystem ioSystem;
	private static final Logger logger = LoggerFactory.getLogger(LandData.class);

	public LandData(ArrayBlockingQueue<String> q, String key, IOSystem ioSystem) {
		this.q = q;
		this.key = key;
		this.ioSystem = ioSystem;
	}

	@Override
	public void run() {
		logger.info("文件落地处理进程，启动！wirte key：" + key);
		while (true) {
			try {
				String record = q.take();
				ioSystem.writeLine(key, record);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

}
