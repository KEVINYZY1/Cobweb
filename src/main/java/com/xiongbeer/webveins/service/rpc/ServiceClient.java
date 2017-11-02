package com.xiongbeer.webveins.service.rpc;

import com.xiongbeer.webveins.service.rpc.LocalWorkerCrawlerServiceBase.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Optional;

/**
 * Created by shaoxiong on 17-10-10.
 */
public enum ServiceClient {
    INSTANCE;

    static final int RPC_SERVER_PORT = 9001;

    Optional<Client> client = Optional.empty();

    Optional<TTransport> tTransport = Optional.empty();

    ServiceClient() {

    }

    public void open() {
        if (isOpened()) {
            return;
        }
        tTransport = Optional.of(new TSocket("localhost", RPC_SERVER_PORT));
        try {
            tTransport.get().open();
            TProtocol protocol = new TBinaryProtocol(tTransport.get());
            client = Optional.of(new LocalWorkerCrawlerServiceBase.Client(protocol));
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    public boolean isOpened() {
        return tTransport.isPresent() && tTransport.get().isOpen();
    }

    public void close() {
        tTransport.ifPresent(TTransport::close);
    }

    public Client operation() {
        return client.get();
    }

    public static void main(String[] args) {
        ServiceClient client = ServiceClient.INSTANCE;
        client.open();
        try {
            System.out.println(client.operation().getNewTask());
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
