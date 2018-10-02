package com.nsn.benchmark.volt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.nsn.iosystem.core.BasicUsageRule;
import com.nsn.iosystem.core.IOSystem;
import com.nsn.iosystem.core.OrdinaryLandRule;
import com.nsn.iosystem.core.RuleFactory;
import com.nsn.iosystem.monitor.DirectoryMonitor;
import com.nsn.pathway.parser.ParserException;
import com.nsn.volt.VoltDB;
import com.nsn.volt.VoltDBPanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author zhangxu
 */
public class Application {
	private static final Logger logger = LoggerFactory.getLogger(Application.class);
	private static String APP_HOME ="";
	private static final int CPU = Runtime.getRuntime().availableProcessors() * 2;
	private Configuration config;
	private DirectoryMonitor monitor;
	// 初始化文件写入
	IOSystem ioSystem = new IOSystem();
	// 获取当前硬盘文件系统
	private FileSystem fileSystem = FileSystems.getDefault();

	private Application() throws IOException {

		loadConfig();
	}

	public static void main(String[] args) throws Exception {
		APP_HOME=args[0];
        new Application().start();
	}

	/**
	 * 加载配置文件
	 *
	 * @throws IOException
	 */
	private void loadConfig() throws IOException {
		logger.info("Loading config.");
		Path configPath = Paths.get(APP_HOME, "conf", "config.yaml");
		config = new ObjectMapper(new YAMLFactory()).readValue(configPath.toFile(), Configuration.class);
	}

	/**
	 * 在文件监听中注册，并连接处理规则
	 *
	 * @param resource
	 *            配置文件资源信息
	 * @param processor
	 *            处理规则
	 * @throws IOException
	 */
	@Deprecated
	private void process(DataResource resource, Consumer<Path> processor) throws IOException {
		PathMatcher matcher = fileSystem.getPathMatcher(resource.getNamePattern());
		monitor.register(Paths.get(resource.getPath()),
				// 以文件全路径或文件名进行测试
				path -> matcher.matches(path) || matcher.matches(path.getFileName()),
				path -> new Thread(() -> processor.andThen(new BackupConsumer(resource.getBackupPolicy())).accept(path)).start(),
				true);
	}

	/**
	 * 监听待回填文件,通过火墙数据回填
	 * 
	 * @param resource
	 *            配置文件资源信息
	 // @param ioSystem
	 *            文件输出工具类
	 * @throws IOException
	 */
	private void processFirewall(DataResource resource) throws IOException {
		PathMatcher matcher = fileSystem.getPathMatcher(resource.getNamePattern());
		monitor.register(Paths.get(resource.getPath()),
				path -> matcher.matches(path) || matcher.matches(path.getFileName()), path -> {
					DataStore.delayFirewall
							.put(new DelayedObject(path.toFile().lastModified() + resource.getDelay(), path));
				}, true);
		for (int i = 0; i < resource.getThreadNum(); i++) {
			new Thread(() -> {
				logger.info("延迟文件处理进程，启动！key：" + resource.getPath());
				for (;;) {
					try {
						DelayedObject obj = DataStore.delayFirewall.poll(100, TimeUnit.MILLISECONDS);
						if (obj != null) {
							Consumer<Path> processor = new S1uDelayProcessor(line -> {
								try {
									DataStore.s1uDelayQueue.put(line);
									Metrics.accumulator_s1udelay.increment();
								} catch (InterruptedException e) {
									logger.error("", e);
								}
							});
							processor.andThen(new BackupConsumer(resource.getBackupPolicy())).accept(obj.getPath());
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}).start();
		}
	}

	private void process(DataResource resource, ArrayBlockingQueue<Path> fileQueue, Consumer<Path> processor)
			throws IOException {
		PathMatcher matcher = fileSystem.getPathMatcher(resource.getNamePattern());
		monitor.register(Paths.get(resource.getPath()),
				path -> matcher.matches(path) || matcher.matches(path.getFileName()), path -> {
					try {
						fileQueue.put(path);
					} catch (InterruptedException e) {
						logger.error("", e);
					}
				}, true);
		for (int i = 0; i < resource.getThreadNum(); i++) {
			new Thread(() -> {
				logger.info("文件处理进程，启动！key：" + resource.getPath());
				for (;;) {
					try {
						Path path = fileQueue.poll(100, TimeUnit.MILLISECONDS);
						if (path != null) {
							processor.andThen(new BackupConsumer(resource.getBackupPolicy())).accept(path);
						}
					} catch (InterruptedException e) {
						logger.error("", e);
					}
				}
			}).start();
		}
	}

	/**
	 * 启动程序
	 *
	 * @throws Exception
	 *             异常想上直接抛出都外部 main 方法
	 */
	public void start() throws Exception {
		// 初始化数据库连接
		VoltDB.voltDBClient = new VoltDBPanel.Builder(config.getConnections()).build();

		// 初始化文件监控
		monitor = new DirectoryMonitor();
		monitor.process();

		// 初始化落地线程
		startOutputThreads(config);

		// for firewall
	//	process(config.getFirewall(), DataStore.firewallFileQueue, new FirewallProcessor());

		// for s1u delay
	//	ioSystem.register("s1u_delay",
	//			createRuleFactory(config.getS1uDelay().getOutput(), config.getS1uDelay().getOutputSuffix()));
	//	processFirewall(config.getS1uDelay());

		// for s6a
		process(config.getS6a(), DataStore.s6aFileQueue, new S6aProcessor());

		// for s11
		ioSystem.register("s11", createRuleFactory(config.getS11().getOutput(), config.getS11().getOutputSuffix()));
		process(config.getS11(), DataStore.s11FileQueue, new S1uS11Processor(line -> {
			try {
				DataStore.s11Queue.put(line);
				Metrics.accumulator_s11.increment();
			} catch (InterruptedException e) {
				logger.error("", e);
			}
		}));

		// for s1u
		ioSystem.register("s1u", createRuleFactory(config.getS1u().getOutput(), config.getS1u().getOutputSuffix()));
		process(config.getS1u(), DataStore.s1uFileQueue, new S1uS11Processor(line -> {
			try {
				DataStore.s1uQueue.put(line);
				Metrics.accumulator_s1u.increment();
			} catch (InterruptedException e) {
				logger.error("", e);
			}
		}));

		// 打印统计信息
		new Thread(new Metrics(config.getMetricInterval())).start();
	}

	/**
	 * 使用输出路径初始化一个文件写入规则
	 *
	 * @param output
	 *            输出路径
	 * @return 文件写入规则
	 */
	private RuleFactory createRuleFactory(String output, String outputSuffix) {
		return new RuleFactory() {
			@Override
			public BasicUsageRule create(String key) throws ParserException {
				// 200M
				return new OrdinaryLandRule(Paths.get(output), "{now(yyyyMMddHHmmss)}_{sequence(all,4)}" + outputSuffix,
						-1, 1024 * 1024 * 200, -1);
			}
		};
	}

	private void startOutputThreads(Configuration config) {
		int s11 = config.getS11().getOutputThreadNum();
		int s1u = config.getS1u().getOutputThreadNum();
		int s1uDelay = config.getS1uDelay().getOutputThreadNum();
		for (int i = 0; i < s11; i++) {
			new Thread(new LandData(DataStore.s11Queue, "s11", ioSystem)).start();
		}
		for (int i = 0; i < s1u; i++) {
			new Thread(new LandData(DataStore.s1uQueue, "s1u", ioSystem)).start();
		}
		for (int i = 0; i < s1uDelay; i++) {
			new Thread(new LandData(DataStore.s1uDelayQueue, "s1u_delay", ioSystem)).start();
		}
	}
}
