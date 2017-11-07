package com.xiongbeer.cobweb.zk.resources;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by shaoxiong on 17-11-5.
 */
public class INode implements INodeAttributes {

    private final Instant createTime;

    private final Instant modificationTime;

    private AtomicBoolean lock = new AtomicBoolean(false);

    private String name;

    private long permission;

    private String path;

    public INode(Instant createTime, Instant modificationTime, String group, int uniqueMarkup) {
        this.createTime = createTime;
        this.modificationTime = modificationTime;
        this.name = Integer.toString(uniqueMarkup);
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public void lock() {
        lock.set(true);
    }

    @Override
    public void unlock() {
        lock.set(false);
    }

    @Override
    public boolean isLocked() {
        return lock.get();
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
