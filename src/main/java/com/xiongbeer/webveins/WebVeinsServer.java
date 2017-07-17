package com.xiongbeer.webveins;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.xiongbeer.webveins.check.SelfTest;
import com.xiongbeer.webveins.saver.DFSManager;
import com.xiongbeer.webveins.saver.HDFSManager;
import com.xiongbeer.webveins.service.protocol.Server;
import com.xiongbeer.webveins.utils.IdProvider;
import com.xiongbeer.webveins.utils.InitLogger;

import org.apache.curator.framework.CuratorFramework;

import com.xiongbeer.webveins.zk.worker.Worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

@SuppressWarnings("restriction")
public class WebVeinsServer {
    private static final Logger logger = LoggerFactory.getLogger(WebVeinsServer.class);
    private static WebVeinsServer wvServer;
    private Server server;
    private Worker worker;
    private String serverId;
    private CuratorFramework client;
    private DFSManager dfsManager;
    private ExecutorService serviceThreadPool = Executors.newFixedThreadPool(1);

    private WebVeinsServer() throws IOException {
        Configuration.getInstance();
        client = SelfTest.checkAndGetZK();
        serverId = new IdProvider().getIp();
        dfsManager = SelfTest.checkAndGetDFS();
        /* 监听kill信号 */
        SignalHandler handler = new StopSignalHandler();
        Signal termSignal = new Signal("TERM");
        Signal.handle(termSignal, handler);
    }

    public static synchronized WebVeinsServer getInstance() throws IOException {
        if (wvServer == null) {
            wvServer = new WebVeinsServer();
        }
        return wvServer;
    }

    public void runServer() throws IOException {
        worker = new Worker(client, serverId);
        server = new Server(Configuration.LOCAL_PORT, client, dfsManager, worker);
        server.bind();
    }

    public void run() {
        /* 主服务 */
        serviceThreadPool.execute(() -> {
            try {
                logger.info("run local server");
                runServer();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        });
    }

    private class StopSignalHandler implements SignalHandler {
        @Override
        public void handle(Signal signal) {
            try {
                logger.info("stoping server...");
                server.stop();
                logger.info("stoping zk client...");
                client.close();
                logger.info("stoping other service...");
                dfsManager.close();
                serviceThreadPool.shutdown();
            } catch (Throwable e) {
                logger.error("handle|Signal handler" + "failed, reason "
                        + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (SelfTest.checkRunning(WebVeinsServer.class.getSimpleName())) {
            logger.error("Service has already running");
            System.exit(1);
        }
        InitLogger.init();
        WebVeinsServer server = WebVeinsServer.getInstance();
        server.run();
    }
}
