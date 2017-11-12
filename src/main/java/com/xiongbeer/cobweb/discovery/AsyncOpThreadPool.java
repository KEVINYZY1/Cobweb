package com.xiongbeer.cobweb.discovery;

import com.xiongbeer.cobweb.conf.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 用于执行不依赖顺序性的操作，防止EventThread阻塞，提高执行效率
 * <p>
 * Created by shaoxiong on 17-5-28.
 */
public class AsyncOpThreadPool {
    private final ExecutorService threadPool;

    private static AsyncOpThreadPool asyncOpThreadPool;

    private Configuration configuration = Configuration.INSTANCE;

    private AsyncOpThreadPool() {
        threadPool = Executors.newFixedThreadPool((Integer) configuration.get("local_async_thread_num"));
    }

    public static synchronized AsyncOpThreadPool getInstance() {
        if (asyncOpThreadPool == null) {
            asyncOpThreadPool = new AsyncOpThreadPool();
        }
        return asyncOpThreadPool;
    }

    public ExecutorService getThreadPool() {
        return threadPool;
    }
}
