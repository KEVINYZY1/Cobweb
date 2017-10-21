package com.xiongbeer.webveins;

import com.xiongbeer.webveins.check.SelfTest;
import com.xiongbeer.webveins.saver.dfs.DFSManager;
import com.xiongbeer.webveins.utils.IdProvider;
import com.xiongbeer.webveins.utils.InitLogger;
import com.xiongbeer.webveins.zk.manager.Manager;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 启动入口
 * Created by shaoxiong on 17-4-20.
 */
@SuppressWarnings("restriction")
public enum WebVeinsMain {
    INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(WebVeinsMain.class);

    private String serverId;

    private Manager manager;

    private DFSManager dfsManager;

    private CuratorFramework client;

    private Configuration configuration;

    private ScheduledExecutorService manageExector = Executors.newScheduledThreadPool(1);

    WebVeinsMain() {
        configuration = Configuration.INSTANCE;
        client = SelfTest.checkAndGetZK();
        serverId = new IdProvider().getIp();
        dfsManager = SelfTest.checkAndGetDFS();
        /* 监听kill信号 */
        SignalHandler handler = new StopSignalHandler();
        Signal termSignal = new Signal("TERM");
        Signal.handle(termSignal, handler);
    }

    /**
     * 定时执行manage
     */
    private void run() {
        manager = Manager.getInstance(client, serverId, dfsManager, configuration.getUrlFilter());
        manageExector.scheduleAtFixedRate(() -> {
            try {
                manager.manage();
            } catch (InterruptedException e) {
                logger.info("shut down.");
            } catch (Throwable e) {
                logger.warn("something wrong when managing: ", e);
            }
        }, 0, configuration.CHECK_TIME, TimeUnit.SECONDS);
    }

    private class StopSignalHandler implements SignalHandler {
        @Override
        public void handle(Signal signal) {
            try {
                logger.info("stoping manager...");
                manager.stop();
                manageExector.shutdownNow();
                client.close();
                dfsManager.close();
            } catch (Throwable e) {
                logger.error("handle|Signal handler" + "failed, reason "
                        + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (SelfTest.checkRunning(WebVeinsMain.class.getSimpleName())) {
            logger.error("Service has already running");
            System.exit(1);
        }
        InitLogger.init();
        WebVeinsMain main = WebVeinsMain.INSTANCE;
        main.run();
    }
}
