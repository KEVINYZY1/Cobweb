package com.xiongbeer.cobweb.api.info;

import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.api.SimpleInfo;
import com.xiongbeer.cobweb.api.jsondata.JData;
import com.xiongbeer.cobweb.api.jsondata.WorkerJson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.LinkedList;
import java.util.List;


/**
 * Created by shaoxiong on 17-5-12.
 */
public class WorkerInfo implements SimpleInfo {
    private List<JData> info;

    private CuratorFramework client;

    public WorkerInfo(CuratorFramework client) {
        this.info = new LinkedList<>();
        this.client = client;
    }

    public WorkerInfo getCurrentWoker() {
        List<String> children;
        try {
            children = client.getChildren().forPath(ZNodeStaticSetting.WORKERS_PATH);
            for (String child : children) {
                WorkerJson data = new WorkerJson();
                Stat stat = new Stat();
                byte[] content;
                content =
                        client.getData()
                                .storingStatIn(stat)
                                .forPath(ZNodeStaticSetting.WORKERS_PATH + '/' + child);
                data.setName(child);
                /* 32为md5码长度 */
                if (content.length == 32) {
                    data.setCurrentTask(new String(content));
                } else {
                    data.setCurrentTask("Empty");
                }
                info.add(data);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public List<JData> getInfo() {
        return info;
    }
}
