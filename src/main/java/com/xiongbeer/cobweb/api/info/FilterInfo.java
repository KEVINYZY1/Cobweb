package com.xiongbeer.cobweb.api.info;

import com.xiongbeer.cobweb.api.SimpleInfo;
import com.xiongbeer.cobweb.api.jsondata.FilterJson;
import com.xiongbeer.cobweb.api.jsondata.JData;
import com.xiongbeer.cobweb.filter.BloomFileInfo;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by shaoxiong on 17-5-12.
 */
public class FilterInfo implements SimpleInfo {
    private DFSManager dfsManager;

    private List<JData> info;

    public FilterInfo(DFSManager dfsManager) {
        this.dfsManager = dfsManager;
        info = new LinkedList<>();
    }

    public FilterInfo getBloomCacheInfo(String src) throws IOException {
        List<String> filesPath = dfsManager.listFiles(src, false);
        for (String path : filesPath) {
            File file = new File(path);
            BloomFileInfo bloomFile = new BloomFileInfo(file.getName());
            FilterJson data = new FilterJson();
            try {
                data.setSize(dfsManager.getFileLen(path));
                data.setMtime(dfsManager.getFileModificationTime(path));
                data.setUniqueID(bloomFile.getUniqueID());
                data.setFpp(bloomFile.getFpp());
                data.setMaxCapacity(bloomFile.getExpectedInsertions());
                data.setUrlsNum(bloomFile.getUrlCounter());
                info.add(data);
            } catch (Exception e) {
                // drop
            }
        }
        return this;
    }

    @Override
    public List<JData> getInfo() {
        return info;
    }
}
