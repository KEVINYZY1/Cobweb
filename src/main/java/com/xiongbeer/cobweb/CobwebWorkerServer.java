package com.xiongbeer.cobweb;

import com.xiongbeer.cobweb.check.SelfTest;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;
import com.xiongbeer.cobweb.service.rpc.ServiceServer;
import com.xiongbeer.cobweb.utils.IdProvider;
import com.xiongbeer.cobweb.utils.InitLogger;
import com.xiongbeer.cobweb.zk.worker.Worker;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings("restriction")
public enum CobwebWorkerServer {
    INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(CobwebWorkerServer.class);

    private static Configuration configuration = Configuration.INSTANCE;

    private static ServiceServer server;

    private Worker worker;

    private String serverId;

    private CuratorFramework client;

    private DFSManager dfsManager;

    private ExecutorService serviceThreadPool = Executors.newFixedThreadPool(1);

    CobwebWorkerServer() {
        client = SelfTest.checkAndGetZK();
        serverId = new IdProvider().getIp();
        dfsManager = SelfTest.checkAndGetDFS();
        /* 监听kill信号 */
        SignalHandler handler = new StopSignalHandler();
        Signal termSignal = new Signal("TERM");
        Signal.handle(termSignal, handler);
    }

    public void runServer() throws IOException {
        worker = new Worker(client, serverId);
        server = ServiceServer.INSTANCE;
        server.init(worker);
        server.run();
    }

    public void run() {
        /* 主服务 */
        serviceThreadPool.execute(() -> {
            try {
                logger.info("run local server");
                runServer();
            } catch (IOException e) {
                logger.error(e.getMessage());
                System.exit(1);
            }
        });
    }

    private class StopSignalHandler implements SignalHandler {
        @Override
        public void handle(Signal signal) {
            try {
                logger.info("stopping server...");
                server.stop();
                logger.info("stopping zk client...");
                client.close();
                logger.info("stopping other service...");
                dfsManager.close();
                serviceThreadPool.shutdown();
            } catch (Throwable e) {
                logger.error("handle|Signal handler" + "failed, reason "
                        + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (SelfTest.checkRunning(CobwebWorkerServer.class.getSimpleName())) {
            logger.error("Service has already running");
            System.exit(1);
        }
        InitLogger.init();
        CobwebWorkerServer.INSTANCE.run();
    }
}
