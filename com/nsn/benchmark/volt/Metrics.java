package com.nsn.benchmark.volt;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 周期打印处理能力
 */
public class Metrics implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(Metrics.class);
	/**
	 * 打印间隔 单位秒
	 */
	private long interval;
	private final String split = "\n";
	private final String lineSep = "----------------------------------------------------------------------------------------------------";

	public Metrics(long interval) {
		this.interval = interval / 1000;
	}

	public static LongAdder accumulator_firewall = new LongAdder();
	public static LongAdder accumulator_s6a = new LongAdder();
	public static LongAdder accumulator_s1u = new LongAdder();
	public static LongAdder accumulator_s11 = new LongAdder();
	public static LongAdder accumulator_s1udelay = new LongAdder();

	public static DoubleAdder firewall_Gbyte = new DoubleAdder();
	public static DoubleAdder s6a_Gbyte = new DoubleAdder();
	public static DoubleAdder s1u_Gbyte = new DoubleAdder();
	public static DoubleAdder s11_Gbyte = new DoubleAdder();
	public static DoubleAdder s1udelay_Gbyte = new DoubleAdder();

	public static AtomicLong file_s1udelay_dealcount = new AtomicLong();

	@Override
	public void run() {
		while (true) {
			try {
				logger.info(getMemory().append(getBusiMeteric()).toString());
				reset();
				TimeUnit.SECONDS.sleep(interval);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 获取内存占用
	 * 
	 * @return 内存信息
	 */
	private StringBuilder getMemory() {
		StringBuilder sb = new StringBuilder();
		sb.append(split);
		sb.append(lineSep).append(split);
		sb.append("totalMemory ");
		sb.append(String.format("%.2f", (double) Runtime.getRuntime().totalMemory() / 1024 / 1024)).append("M ");
		sb.append("| freeMemory ");
		sb.append(String.format("%.2f", (double) Runtime.getRuntime().freeMemory() / 1024 / 1024)).append("M ");
		sb.append("| every ").append(interval).append(" seconds").append(split);
		sb.append(lineSep).append(split);
		return sb;
	}

	/**
	 * 打印业务指标 累计处理量，每秒处理能力
	 * 
	 * @return 业务日志
	 */
	private StringBuilder getBusiMeteric() {
		StringBuilder sb = new StringBuilder();
		sb.append("LOAD | firewall total(G) ");
		sb.append(String.format("%.2f", firewall_Gbyte.doubleValue()));
		sb.append(", speed(line/s) ");
		sb.append(accumulator_firewall.longValue() / interval).append(" ");
		sb.append("| s6a total(G) ");
		sb.append(String.format("%.2f", s6a_Gbyte.doubleValue()));
		sb.append(", speed(line/s) ");
		sb.append(accumulator_s6a.longValue() / interval).append(split);
		sb.append("FILLBACK1 | s11 total(G) ");
		sb.append(String.format("%.2f", s11_Gbyte.doubleValue()));
		sb.append(", speed(line/s) ");
		sb.append(accumulator_s11.longValue() / interval).append(" ");
		sb.append("| s1u total(G) ");
		sb.append(String.format("%.2f", s1u_Gbyte.doubleValue()));
		sb.append(", speed(line/s) ");
		sb.append(accumulator_s1u.longValue() / interval).append(" ");
		sb.append(split);
		sb.append("FILLBACK2 | firewall total(G) ");
		sb.append(String.format("%.2f", s1udelay_Gbyte.doubleValue()));
		sb.append(", speed(line/s) ");
		sb.append(accumulator_s1udelay.longValue() / interval).append(" ");
		sb.append(split);
		sb.append("FILE | s1udelay " + DataStore.delayFirewall.size()).append(" ");
		sb.append("| deal ").append(file_s1udelay_dealcount.get()).append(" ");
		sb.append("| firewall ").append(DataStore.firewallFileQueue.size()).append(" ");
		sb.append("| s11 ").append(DataStore.s11FileQueue.size()).append(" ");
		sb.append("| s6a ").append(DataStore.s6aFileQueue.size()).append(" ");
		sb.append("| s1u ").append(DataStore.s1uFileQueue.size());
		sb.append(split);
		sb.append("OUTPUTQUEUE | s11 ").append(DataStore.s11Queue.size()).append(" | ");
		sb.append("s1u ").append(DataStore.s1uQueue.size()).append(" | ");
		sb.append("s1udelay ").append(DataStore.s1uDelayQueue.size());
		return sb;
	}

	/**
	 * 重置计数器
	 */
	private void reset() {
		accumulator_firewall = new LongAdder();
		accumulator_s6a = new LongAdder();
		accumulator_s1u = new LongAdder();
		accumulator_s11 = new LongAdder();
		accumulator_s1udelay = new LongAdder();
	}
}
