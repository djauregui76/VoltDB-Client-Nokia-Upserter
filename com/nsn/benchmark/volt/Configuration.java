package com.nsn.benchmark.volt;

/**
 * @author zhangxu
 */
public class Configuration {
    /**
     * 数据库连接地址
     */
    private String connections;
    /**
     * 统计打印间隔毫秒
     */
    private long metricInterval = 60000;
    
	/**
     * 相关类型文件处理信息
     */
    private DataResource firewall;
    private DataResource s1u;
    private DataResource s1uDelay;
    private DataResource s6a;
    private DataResource s11;

    public String getConnections() {
        return connections;
    }

    public Configuration setConnections(String connections) {
        this.connections = connections;
        return this;
    }

    public DataResource getFirewall() {
        return firewall;
    }

    public Configuration setFirewall(DataResource firewall) {
        this.firewall = firewall;
        return this;
    }

    public DataResource getS1u() {
        return s1u;
    }

    public Configuration setS1u(DataResource s1u) {
        this.s1u = s1u;
        return this;
    }

    public DataResource getS1uDelay() {
        return s1uDelay;
    }

    public Configuration setS1uDelay(DataResource s1uDelay) {
        this.s1uDelay = s1uDelay;
        return this;
    }

    public DataResource getS6a() {
        return s6a;
    }

    public Configuration setS6a(DataResource s6a) {
        this.s6a = s6a;
        return this;
    }

    public DataResource getS11() {
        return s11;
    }

    public Configuration setS11(DataResource s11) {
        this.s11 = s11;
        return this;
    }
    
    public long getMetricInterval() {
		return metricInterval;
	}

	public void setMetricInterval(long metricInterval) {
		this.metricInterval = metricInterval;
	}

}
