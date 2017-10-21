package com.xiongbeer.webveins.service.rpc;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by shaoxiong on 17-10-10.
 */
public enum ServiceClient {
    INSTANCE;

    public static void main(String[] args) {
        TTransport tTransport = new TSocket("localhost", 9000);
        try {
            tTransport.open();
            TProtocol protocol = new TBinaryProtocol(tTransport);
            LocalWorkerCrawlerService.Client client = new LocalWorkerCrawlerService.Client(protocol);
            System.out.println(client.getManagersStatus());
            tTransport.close();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
