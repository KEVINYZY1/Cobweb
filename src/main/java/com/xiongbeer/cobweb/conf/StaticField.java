package com.xiongbeer.cobweb.conf;

/**
 * Created by shaoxiong on 17-11-6.
 */
final public class StaticField {
    private StaticField() {

    }

    public static final String ZK_SERVER_FILE_NAME = "zk_server";

    public static final String TEMP_SUFFIX = ".bak";

    public static final String BLOOM_CACHE_FILE_PREFIX = "bloom_cache";

    public static final String BLOOM_CACHE_FILE_SUFFIX = ".dat";

    public static final String SLICE_FILE_PREFIX = "slice";

    public static final int DEFAULT_FILTER_MARKUP = 0;
}
