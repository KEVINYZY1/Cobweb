package com.xiongbeer.cobweb.discovery.resources;

import java.time.Instant;

/**
 * Created by shaoxiong on 17-11-7.
 */
public class FilterINode extends INode implements FilterFields {

    public FilterINode(Instant createTime, Instant modificationTime, String group, int uniqueMarkup) {
        super(createTime, modificationTime, group, uniqueMarkup);
    }

    @Override
    public boolean mount() {
        return false;
    }

    @Override
    public boolean unmount() {
        return false;
    }

    @Override
    public Object getFilterInfo() {
        return null;
    }
}
