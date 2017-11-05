package com.xiongbeer.cobweb.zk.task;

import org.junit.Test;

/**
 * Created by shaoxiong on 17-6-7.
 */
public class TaskDataTest {

    @Test
    public void parseTest() {
        TaskData data = new TaskData();
        data.setStatus(Task.Status.WAITING).setProgress(300).setUniqueMarkup(200);
        TaskData data1 = new TaskData(data.getBytes());
        System.out.println(data1);
    }
}
