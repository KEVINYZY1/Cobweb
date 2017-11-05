package com.xiongbeer.cobweb.zk.resources;

import java.time.Instant;

/**
 * Created by shaoxiong on 17-11-5.
 */
public class INode implements INodeAttributes {

    private final Instant createTime;

    private final Instant modificationTime;

    private String name;

    private long permission;

    public INode(Instant createTime, Instant modificationTime) {
        this.createTime = createTime;
        this.modificationTime = modificationTime;
    }


    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isLocked() {
        return false;
    }

    @Override
    public String getMarkup() {
        return null;
    }

    @Override
    public String getGroup() {
        return null;
    }
}
