package com.xiongbeer.webveins.service.rpc;

import com.xiongbeer.webveins.service.rpc.LocalWorkerCrawlerService.Iface;
import com.xiongbeer.webveins.service.rpc.LocalWorkerCrawlerService.Processor;
import com.xiongbeer.webveins.zk.worker.Worker;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Created by shaoxiong on 17-10-10.
 */
public enum ServiceServer {
    INSTANCE();

    final Logger logger = LoggerFactory.getLogger(ServiceServer.class);

    ServiceServer() {

    }

    LocalWorkerCrawlerService.Processor<WorkerCrawlerServiceImpl> processor;

    WorkerCrawlerServiceImpl impl;

    Optional<TServer> server;

    static final int RPC_SERVER_PORT = 9000;

    private void simple(Processor<Iface> processor, Worker worker) {
        if (isServing()) {
            return;
        }
        try {
            impl = new WorkerCrawlerServiceImpl(worker);
            processor = new LocalWorkerCrawlerService.Processor<>(impl);
            TServerTransport serverTransport = new TServerSocket(RPC_SERVER_PORT);
            server = Optional.of(
                    new TThreadPoolServer(
                            new TThreadPoolServer.Args(serverTransport).processor(processor)));
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    boolean isServing() {
        if (server.isPresent() && server.get().isServing()) {
            return true;
        }
        return false;
    }

    void stop() {
        logger.info("Stopping the local worker-crawler server...");
        server.ifPresent(TServer::stop);
    }

    void run() {
        logger.info("Starting the local worker-crawler server...");
        server.ifPresent(TServer::serve);
    }

    /*SSL TODO*/
    public void secure(LocalWorkerCrawlerService.Processor<LocalWorkerCrawlerService.Iface> processor) {

    }
}