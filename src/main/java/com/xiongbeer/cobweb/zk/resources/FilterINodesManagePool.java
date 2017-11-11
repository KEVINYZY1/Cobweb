package com.xiongbeer.cobweb.zk.resources;

import com.xiongbeer.cobweb.conf.Configuration;
import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.exception.CobwebRuntimeException;
import com.xiongbeer.cobweb.filter.URIBloomFilter;
import com.xiongbeer.cobweb.log.RelaxedTransaction;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by shaoxiong on 17-11-7.
 */
public enum FilterINodesManagePool {
    INSTANCE;

    private Map<Integer, INode> nodesPool;

    private CuratorFramework client;

    private static final Logger logger = LoggerFactory.getLogger(FilterINodesManagePool.class);

    private Configuration configuration = Configuration.INSTANCE;

    FilterINodesManagePool() {
        nodesPool = new ConcurrentHashMap<>();
    }

    public void init(CuratorFramework client) {
        this.client = client;
        logger.info("loading filters' node information...");
        loadExistNodes();
    }

    public void loadExistNodes() {
        Stat tmpStat = new Stat();
        try {
            List<String> groups = client.getChildren()
                    .forPath(ZNodeStaticSetting.FILTERS_ROOT);
            for (String group : groups) {
                List<String> nodes = client.getChildren()
                        .forPath(makeFilterZNodePath(group));
                for (String node : nodes) {
                    int markup = Integer.parseInt(node);
                    client.getData()
                            .storingStatIn(tmpStat)
                            .forPath(makeFilterZNodePath(group, markup));
                    FilterINode inode = new FilterINode(Instant.ofEpochMilli(tmpStat.getCtime())
                            , Instant.ofEpochMilli(tmpStat.getMtime()), group, markup);
                    nodesPool.put(markup, inode);
                }
            }
        } catch (KeeperException.ConnectionLossException e) {
            logger.warn("retrying : loadExistNodes");
            loadExistNodes();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void createNewBloomFilter(long expectedInsertions, double fpp, String group, int uniqueMarkup) {
        URIBloomFilter bloomFilter = new URIBloomFilter(expectedInsertions, fpp);
        RelaxedTransaction.doProcess(() -> {
            try {
                bloomFilter.save((String) configuration.get("bloom_local_save_path"));
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(makeFilterZNodePath(group, uniqueMarkup));
            } catch (Exception e) {
                throw new CobwebRuntimeException
                        .OperationFailedException("create a new filter node failed. \n" + e.getMessage());
            }
        }, bloomFilter::delete, logger);
        INode newNode = new FilterINode(Instant.now(), Instant.now(), group, uniqueMarkup);
        nodesPool.put(uniqueMarkup, newNode);
    }

    private String makeFilterZNodePath(String group, int uniqueMarkup) {
        return ZNodeStaticSetting.FILTERS_ROOT + ZNodeStaticSetting.PATH_SEPARATOR + group
                + ZNodeStaticSetting.PATH_SEPARATOR + Integer.toString(uniqueMarkup);
    }

    private String makeFilterZNodePath(String group) {
        return ZNodeStaticSetting.FILTERS_ROOT + ZNodeStaticSetting.PATH_SEPARATOR + group;
    }
}
