package com.xiongbeer.cobweb.zk.resources;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by shaoxiong on 17-11-7.
 */
public enum FilterINodesManagePool {
    INSTANCE;

    private Map<INode, Integer> nodesPool;

    FilterINodesManagePool() {
        nodesPool = new ConcurrentHashMap<>();
    }


}
