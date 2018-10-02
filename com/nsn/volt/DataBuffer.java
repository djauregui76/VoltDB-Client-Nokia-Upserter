package com.nsn.volt;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 批量执行操作，继承 {@link CopyOnWriteArrayList}, 数据长度超出时自动调用处理方法
 *
 * @author zhangxu
 */
public class DataBuffer<E> extends CopyOnWriteArrayList<E> {
    private int limit;
    private Consumer<List<E>> callback;
    private Sluice sluice = new Sluice();

    public DataBuffer(Consumer<List<E>> callback) {
        this(callback, 6500);
    }

    public DataBuffer(Consumer<List<E>> callback, int limit) {
        this.callback = callback;
        this.limit = limit;
    }

    @Override
    public synchronized boolean add(E e) {
        return sluice.apply(super.add(e));
    }

    /**
     * 处理剩余内容
     */
    public final synchronized void flush() {
        if (this.size() > 0) {
            callback.accept(this);
            clear();
        }
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public Consumer<List<E>> getCallback() {
        return callback;
    }

    public void setCallback(Consumer<List<E>> callback) {
        this.callback = callback;
    }

    private class Sluice {
        <T> T apply(T t) {
            if (size() >= limit) {
                callback.accept(DataBuffer.this);
                clear();
            }
            return t;
        }
    }
}
