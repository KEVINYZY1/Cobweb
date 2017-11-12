package com.xiongbeer.cobweb.discovery.manager;

import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.discovery.AsyncOpThreadPool;
import com.xiongbeer.cobweb.discovery.task.TaskManager;
import com.xiongbeer.cobweb.discovery.worker.WorkersWatcher;
import com.xiongbeer.cobweb.exception.CobwebRuntimeException;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;
import com.xiongbeer.cobweb.utils.Async;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author shaoxiong
 *         Manager用于管理整个事务
 *         其中active_manager为活动的Server，而standby manager
 *         监听active manager，一旦活动节点失效则接管其工作
 */
public class Manager {
    /**
     * Manager的状态
     */
    public enum Status {
        /*
            INITIALIZING: 刚初始化，还未进行选举
         */
        INITIALIZING,
        /*
            ELECTED: 主节点
        */
        ELECTED,
        /*
            NOT_ELECTED: 从节点
        */
        NOT_ELECTED,
        /*
            RECOVERING: 检测到主节点死亡，尝试恢复中
        */
        RECOVERING
    }

    public static Manager manager;

    private static final Logger logger = LoggerFactory.getLogger(Manager.class);

    private CuratorFramework client;

    private String serverId;

    private String managerPath;

    private Status status;

    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);

    private ExecutorService asyncOpThreadPool = AsyncOpThreadPool.getInstance().getThreadPool();

    private ManageTransaction manageTransaction;

    private Manager(CuratorFramework client, String serverId,
                    DFSManager dfsManager) {
        this.client = client;
        this.serverId = serverId;
        this.manageTransaction =
                new ManageTransaction(dfsManager, new TaskManager(client), new WorkersWatcher(client));
        this.status = Status.INITIALIZING;
        toBeActive();
    }

    public void stop() {
        asyncOpThreadPool.shutdownNow();
    }

    public static synchronized
    Manager getInstance(CuratorFramework client, String serverId, DFSManager dfsManager) {
        if (manager == null) {
            manager = new Manager(client, serverId, dfsManager);
        }
        return manager;
    }

    public Status getStatus() {
        return status;
    }

    public String getManagerPath() {
        return managerPath;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    /**
     * 核心方法，manager定期进行manage：
     * 1.刷新任务列表
     * 2.检查Worker
     * 3.发布新的任务
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws CobwebRuntimeException.FilterOverflowException bloomFilter的容量已满
     */
    public void manage() throws InterruptedException
            , IOException, CobwebRuntimeException.FilterOverflowException {
        if (status == Status.ELECTED) {
            logger.debug("start manage process...");

        }
    }

    /**
     * TODO
     * 接管active职责
     * <p>
     * 平稳地将当前active manager注销，然后
     * 让standby manager接管它的工作
     */
    public void takeOverResponsibility() {
        // 只有standby节点才能接管
        if (status == Status.NOT_ELECTED) {

        }
    }

    /**
     * activeManager的监听器
     * <p>
     * 当其被删除时(失效时)，就开始尝试让
     * 活动的standbyManager(中的某一个)
     * 来接管失效的activeManager
     */
    private Watcher actManagerExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                assert ZNodeStaticSetting.ACTIVE_MANAGER_PATH.equals(watchedEvent.getPath());
                logger.warn("Active manager deleted, now trying to activate manager again. by server."
                        + serverId + " ...");
                recoverActiveManager();
            }
        }
    };

    /**
     * standbyManager的监听器
     * <p>
     * 失效时，尝试重新连接
     */
    private Watcher stdManagerExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            assert managerPath.equals(watchedEvent.getPath());
            if (status == Status.NOT_ELECTED) {
                logger.warn("standby manager deleted, now trying to recover it. by server."
                        + serverId + " ...");
                toBeStandBy();
            }
        }
    };

    /**
     * 集合操作的Callback，尝试恢复activeManager
     * <p>
     * 需要先删除之前自身创建的
     * standby_manager节点，然后
     * 创建active_manager节点。
     * 这2个操作中的任何一个操作失败，
     * 则整个操作失败。
     */
    private BackgroundCallback recoverMultiCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        logger.warn("CONNECTIONLOSS, retrying to recover active manager. server."
                                + serverId + " ...");
                        recoverActiveManager();
                        break;
                    case OK:
                        status = Status.ELECTED;
                        managerPath = ZNodeStaticSetting.ACTIVE_MANAGER_PATH;
                        logger.info("Recover active manager success. now server." + serverId
                                + " is active manager.");
                        activeManagerExists();
                        break;
                    case NODEEXISTS:
                        status = Status.NOT_ELECTED;
                        logger.info("Active manager has already recover by other server.");
                        activeManagerExists();
                        break;
                    default:
                        status = Status.NOT_ELECTED;
                        logger.error("Something went wrong when recoving for active manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };


    /*
     * 恢复active_manager
     *
     * status记录了此节点之前是否是active状态，
     * 是则立刻重新获取active权利，否则先等待
     * JITTER_DELAY秒，然后尝试获取active权利
     *
     * 这样做的原因是为了防止网络抖动造成的
     * active_manager被误杀
     */
    @Async
    private void recoverActiveManager() {
        if (status == Status.NOT_ELECTED) {
            status = Status.RECOVERING;
            delayExector.schedule(() -> {
                try {
                    CuratorOp deleteOp = client.transactionOp()
                            .delete()
                            .forPath(managerPath);
                    CuratorOp createOp = client.transactionOp()
                            .create()
                            .withMode(CreateMode.EPHEMERAL)
                            .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
                    client.transaction()
                            .inBackground(recoverMultiCallback, asyncOpThreadPool)
                            .forOperations(deleteOp, createOp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, ZNodeStaticSetting.JITTER_DELAY, TimeUnit.SECONDS);
        } else {
            toBeActive();
        }
    }

    private BackgroundCallback actManagerExistsCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                Stat stat = curatorEvent.getStat();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        activeManagerExists();
                        break;
                    case OK:
                        if (stat == null) {
                            recoverActiveManager();
                            break;
                        }
                        break;
                    default:
                        checkActiveManager();
                        break;
                }
            };

    /**
     * 检查active_manager节点是否还存在
     * 并且设置监听点
     */
    @Async
    private void activeManagerExists() {
        Watcher watcher = status == Status.NOT_ELECTED ?
                actManagerExistsWatcher : null;
        try {
            client.checkExists()
                    .usingWatcher(watcher)
                    .inBackground(actManagerExistsCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
        } catch (Exception e) {
            logger.warn("Unknow error.", e);
        }
    }

    private BackgroundCallback stdManagerExistsCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        standbyManagerExists();
                        break;
                    case OK:
                        // pass
                        break;
                    case NONODE:
                /* 有可能是standy节点转为了active状态，那个时候便不需要重新设置standby节点 */
                        if (status == Status.NOT_ELECTED) {
                            toBeStandBy();
                            logger.warn("standby manager deleted, now trying to recover it. by server."
                                    + serverId + " ...");
                        }
                        break;
                    default:
                        logger.error("Something went wrong when check standby manager itself.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 检查自身standby_manager节点是否还存在
     * 并且设置监听点
     */
    @Async
    private void standbyManagerExists() {
        try {
            client.checkExists()
                    .usingWatcher(stdManagerExistsWatcher)
                    .inBackground(stdManagerExistsCallback, asyncOpThreadPool)
                    .forPath(managerPath);
        } catch (Exception e) {
            logger.warn("Unknow error.", e);
        }
    }

    private BackgroundCallback actManagerCreateCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        checkActiveManager();
                        break;
                    case OK:
                        logger.info("Active manager created success. at {}", new Date().toString());
                        managerPath = path;
                        status = Status.ELECTED;
                        activeManagerExists();
                        break;
                    case NODEEXISTS:
                        logger.info("Active manger already exists, turn to set standby manager...");
                        toBeStandBy();
                        break;
                    default:
                        logger.error("Something went wrong when running for active manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 激活active_manager
     */
    @Async
    private void toBeActive() {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .inBackground(actManagerCreateCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
        } catch (Exception e) {
            logger.warn("unknow error.", e);
        }
    }

    private BackgroundCallback stdManagerCreateCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        toBeStandBy();
                        break;
                    case OK:
                        status = Status.NOT_ELECTED;
                        managerPath = path;
                        logger.info("Server." + serverId + " registered. at {}", new Date().toString());
                        activeManagerExists();
                        standbyManagerExists();
                        break;
                    case NODEEXISTS:
                        //TODO
                        break;
                    default:
                        logger.error("Something went wrong when running for stand manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 激活standby_manager
     */
    @Async
    private void toBeStandBy() {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .inBackground(stdManagerCreateCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.STANDBY_MANAGER_PATH + serverId);
        } catch (Exception e) {
            logger.warn("unknow error.", e);
        }
    }

    private BackgroundCallback actCheckCallback =
            (CuratorFramework curatorFramework, CuratorEvent curatorEvent) -> {
                int rc = curatorEvent.getResultCode();
                String path = curatorEvent.getPath();
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        checkActiveManager();
                        break;
                    case NONODE:
                        recoverActiveManager();
                        break;
                    default:
                        logger.error("Something went wrong when check active manager.",
                                KeeperException.create(Code.get(rc), path));
                        break;
                }
            };

    /**
     * 检查active_manager的状态
     */
    @Async
    private void checkActiveManager() {
        try {
            client.getData()
                    .inBackground(actCheckCallback, asyncOpThreadPool)
                    .forPath(ZNodeStaticSetting.ACTIVE_MANAGER_PATH);
        } catch (Exception e) {
            logger.warn("unknow error.", e);
        }
    }
}
