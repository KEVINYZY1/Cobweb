package com.xiongbeer.webveins.filter;

import java.io.IOException;

public interface Filter {
    /**
     * 将URI放入filter
     *
     * @param str
     * @return
     */
    boolean put(String str) throws IOException;

    /**
     * 判断URI是否已存在于filter
     *
     * @param str
     * @return
     */
    boolean exist(String str) throws IOException;

    /**
     * 序列化filter并存储到本地磁盘
     *
     * @param dst
     * @return
     * @throws IOException
     */
    String save(String dst) throws IOException;

    /**
     * 反序列化在本地磁盘上的filter文件
     *
     * @param src
     * @throws IOException
     */
    void load(String src) throws IOException;
}
