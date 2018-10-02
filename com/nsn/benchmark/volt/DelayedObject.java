package com.nsn.benchmark.volt;

import java.nio.file.Path;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 延迟处理对象
 *
 */
public class DelayedObject implements Delayed {
	// 触发处理时间
	private long time;
	// 文件路径
	private Path path;

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public DelayedObject(long time, Path path) {
		this.time = time;
		this.path = path;
	}

	@Override
	public int compareTo(Delayed arg0) {
		// TODO Auto-generated method stub
		if (arg0 instanceof DelayedObject) {
			DelayedObject s = (DelayedObject) arg0;
			if (this.time > s.time) {
				return 1;
			} else if (this.time == s.time) {
				return 0;
			} else {
				return -1;
			}
		}
		return 0;
	}

	@Override
	public long getDelay(TimeUnit arg0) {
		// TODO Auto-generated method stub
		return arg0.convert(time - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

}
