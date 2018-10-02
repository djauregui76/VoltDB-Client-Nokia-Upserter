package com.nsn.benchmark.volt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @author zhangxu
 */
@JsonSerialize
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataResource {
	/**
	 * 文件采集路径
	 */
	private String path;
	/**
	 * 文件名匹配模式
	 */
	private String namePattern;
	/**
	 * 文件备份规则
	 */
	private String backupPolicy;
	/**
	 * 文件输出路径
	 */
	private String output;
	/**
	 * 处理线程数 默认1个
	 */
	private int threadNum = 1;

	private int outputThreadNum = 1;

	public int getOutputThreadNum() {
		return outputThreadNum;
	}

	public void setOutputThreadNum(int outputThreadNum) {
		this.outputThreadNum = outputThreadNum;
	}

	/**
	 * 延迟输出毫秒 默认25分钟
	 */
	private long delay = 1500000;
	/**
	 * 文件输出后缀
	 */
	private String outputSuffix = ".txt";

	public String getOutputSuffix() {
		return outputSuffix;
	}

	public void setOutputSuffix(String outputSuffix) {
		this.outputSuffix = outputSuffix;
	}

	public long getDelay() {
		return delay;
	}

	public void setDelay(long delay) {
		this.delay = delay;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	public String getPath() {
		return path;
	}

	public DataResource setPath(String path) {
		this.path = path;
		return this;
	}

	public String getNamePattern() {
		return namePattern;
	}

	public DataResource setNamePattern(String namePattern) {
		this.namePattern = namePattern;
		return this;
	}

	public String getBackupPolicy() {
		return backupPolicy;
	}

	public DataResource setBackupPolicy(String backupPolicy) {
		this.backupPolicy = backupPolicy;
		return this;
	}

	public String getOutput() {
		return output;
	}

	public DataResource setOutput(String output) {
		this.output = output;
		return this;
	}
}
