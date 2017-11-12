package com.xiongbeer.cobweb.conf;

/**
 * Created by shaoxiong on 17-4-6.
 */
final public class ZNodeStaticSetting {
    private ZNodeStaticSetting() {

    }

    public static final String PATH_SEPARATOR = "/";

    public static final String ROOT_PATH = "/webveins";

    public static final String WORKERS_PATH = "/wvWorkers";

    public static final String TASKS_PATH = "/wvTasks";

    public static final String MANAGERS_PATH = "/wvManagers";

    public static final String FILTERS_ROOT = "/wvFilters";

    public static final String NEW_WORKER_PATH = WORKERS_PATH + "/worker_";

    public static final String ACTIVE_MANAGER_PATH = MANAGERS_PATH + "/active_manager";

    public static final String STANDBY_MANAGER_PATH = MANAGERS_PATH + "/standby_manager_";

    public static final String NEW_TASK_PATH = TASKS_PATH + "/";

    public static final int JITTER_DELAY = 10;
}