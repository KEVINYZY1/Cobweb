package com.xiongbeer.cobweb.zk.resources;

import java.time.Instant;
import java.util.List;

/**
 * Created by shaoxiong on 17-11-5.
 */
public class INodeDirectory implements INodeAttributes {
    private final Instant createTime;

    private final Instant modificationTime;

    private String name;

    private long permission;

    private List<INodeAttributes> children = null;

    public INodeDirectory(Instant createTime, Instant modificationTime, String name) {
        this.createTime = createTime;
        this.modificationTime = modificationTime;
        this.name = name;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public String getPath() {
        return null;
    }

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

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
