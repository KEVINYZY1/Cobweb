package com.xiongbeer.cobweb.discovery.resources;

/**
 * Created by shaoxiong on 17-11-7.
 */
public interface FilterFields extends AdditionalFields {
    boolean mount();

    boolean unmount();

    Object getFilterInfo();
}
