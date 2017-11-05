package com.xiongbeer.cobweb.zk.resources;

/**
 * Created by shaoxiong on 17-11-5.
 */
public interface INodeAttributes {
    boolean isDirectory();

    boolean isLocked();

    String getMarkup();

    String getGroup();
}
