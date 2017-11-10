package com.xiongbeer.cobweb.service.local;

import com.xiongbeer.cobweb.Configuration;
import com.xiongbeer.cobweb.service.protocol.Client;
import com.xiongbeer.cobweb.service.protocol.message.MessageType;
import com.xiongbeer.cobweb.service.protocol.message.ProcessDataProto.ProcessData;
import com.xiongbeer.cobweb.utils.InitLogger;

/**
 * Created by shaoxiong on 17-6-2.
 */
public class ShellBootstrap extends Bootstrap {
    private static Configuration configuration = Configuration.INSTANCE;

    public ShellBootstrap(String command){
        ProcessData.Builder builder = ProcessData.newBuilder();
        builder.setType(MessageType.SHELL_REQ.getValue());
        builder.setCommand(command);
        super.client = new Client(builder.build());
    }

    @Override
    public void ready() {
        init();
    }

    public static void main(String[] args){
        InitLogger.initEmpty();
        StringBuilder command = new StringBuilder();
        for(String arg:args){
            command.append(arg);
            command.append(" ");
        }
        System.out.println("[info] command: " + command.toString());
        //Bootstrap bootstrap = new ShellBootstrap(command.toString());
        Bootstrap bootstrap = new ShellBootstrap("listtasks");
        bootstrap.ready();
    }
}
