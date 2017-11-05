package com.xiongbeer.cobweb.service.protocol.handler;

import com.xiongbeer.cobweb.Configuration;
import com.xiongbeer.cobweb.api.Command;
import com.xiongbeer.cobweb.api.OutputFormatter;
import com.xiongbeer.cobweb.api.info.FilterInfo;
import com.xiongbeer.cobweb.api.info.TaskInfo;
import com.xiongbeer.cobweb.api.info.WorkerInfo;
import com.xiongbeer.cobweb.api.job.DFSJob;
import com.xiongbeer.cobweb.api.job.TaskJob;
import com.xiongbeer.cobweb.api.jsondata.JData;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;
import com.xiongbeer.cobweb.service.protocol.message.MessageType;
import com.xiongbeer.cobweb.service.protocol.message.ProcessDataProto.ProcessData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shaoxiong on 17-5-30.
 */
public class ShellRespHandler extends ChannelInboundHandlerAdapter {
    private static Configuration configuration = Configuration.INSTANCE;

    private CuratorFramework client;

    private DFSManager dfsManager;

    public ShellRespHandler(CuratorFramework zk, DFSManager dfsManager) {
        this.client = zk;
        this.dfsManager = dfsManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ProcessData req = (ProcessData) msg;
        if (req.getType() == MessageType.SHELL_REQ.getValue()) {
            Command result = analysis(req.getCommand());
            //TODO add args
            ProcessData resp = buildShellResp(operation(result));
            ctx.writeAndFlush(resp);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
    }

    private ProcessData buildShellResp(String content) {
        ProcessData.Builder builder = ProcessData.newBuilder();
        builder.setType(MessageType.SHELL_RESP.getValue());
        builder.setCommandReasult(content);
        return builder.build();
    }

    private Command analysis(String req) {
        EnumSet<Command> commands = EnumSet.allOf(Command.class);
        for (Command command : commands) {
            Pattern pattern = Pattern.compile(command.toString());
            Matcher matcher = pattern.matcher(req.toUpperCase());
            while (matcher.find()) {
                return command;
            }
        }
        return null;
    }

    private String operation(Command command, String... args) {
        List<JData> dataSet = null;
        String result = null;
        if (command == null) {
            return "[Error] Empty input";
        }
        switch (command) {
            case LISTTASKS:
                TaskInfo taskInfo = new TaskInfo(client);
                dataSet = taskInfo.getCurrentTasks().getInfo();
                result = JDecoder(dataSet);
                break;
            case LISTFILTERS:
                FilterInfo filterInfo = new FilterInfo(dfsManager);
                try {
                    dataSet = filterInfo
                            .getBloomCacheInfo(configuration.BLOOM_BACKUP_PATH)
                            .getInfo();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                result = JDecoder(dataSet);
                break;
            case LISTWORKERS:
                WorkerInfo workerInfo = new WorkerInfo(client);
                dataSet = workerInfo.getCurrentWoker().getInfo();
                result = JDecoder(dataSet);
                break;
            case REMOVETASKS:
                TaskJob taskJob = new TaskJob(client, dfsManager);
                if (args.length >= 2) {
                    result += taskJob.removeTasks(args[1]);
                } else {
                    return "[Error] lack of args";
                }
                result += "Done.";
                break;
            case EMPTYHDFSTRASH:
                DFSJob hdfsJob = new DFSJob(dfsManager);
                hdfsJob.EmptyTrash();
                result = "Done.";
                break;
            default:
                break;
        }
        return result;
    }

    private String JDecoder(List<JData> dataSet) {
        if (dataSet == null || dataSet.size() == 0) {
            return "Null dataSet";
        }
        return new OutputFormatter(dataSet).format();
    }
}
