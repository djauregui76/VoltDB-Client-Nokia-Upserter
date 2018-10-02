package com.nsn.benchmark.volt;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.DelayQueue;


public class DataStore {
	public static ArrayBlockingQueue<String> s11Queue = new ArrayBlockingQueue<String>(500000);
	public static ArrayBlockingQueue<String> s1uQueue = new ArrayBlockingQueue<String>(500000);
	public static ArrayBlockingQueue<String> s1uDelayQueue = new ArrayBlockingQueue<String>(500000);
	public static DelayQueue<DelayedObject> delayFirewall = new DelayQueue<DelayedObject>();
	
	public static ArrayBlockingQueue<Path> s11FileQueue = new ArrayBlockingQueue<Path>(10000);
	public static ArrayBlockingQueue<Path> s1uFileQueue = new ArrayBlockingQueue<Path>(10000);
	public static ArrayBlockingQueue<Path> s6aFileQueue = new ArrayBlockingQueue<Path>(10000);
	public static ArrayBlockingQueue<Path> firewallFileQueue = new ArrayBlockingQueue<Path>(10000);
}
