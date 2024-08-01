package com.zhuravishkin.demo_zookeeper_sftp.domain;

import com.google.common.io.Files;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.zhuravishkin.demo_zookeeper_sftp.config.DomainProperties;
import com.zhuravishkin.demo_zookeeper_sftp.config.SftpConfiguration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.zhuravishkin.demo_zookeeper_sftp.util.Util.*;

@Slf4j
public class SftpConnector implements CuratorWatcher {
    private SftpConfiguration sftpConfiguration;
    private DomainProperties properties;
    private final ScheduledExecutorService delayedStartScheduler;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> delayedStartFutureTask = null;
    private ScheduledFuture<?> scheduledFuture;
    private CuratorFramework zkClient;
    private String zkNodesPath;
    private String zkLockPath;
    private List<String> nodes;
    private String nodeNumber;
    private boolean isDisconnected;
    private final Object zkLock = new Object();

    public SftpConnector() {
        try {
            delayedStartScheduler = Executors.newScheduledThreadPool(1);
            executor = Executors.newScheduledThreadPool(1);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void setSftpConfiguration(SftpConfiguration configuration) {
        this.sftpConfiguration = configuration;
    }

    public void setProperties(DomainProperties properties) {
        try {
            this.properties = properties;
            isDisconnected = false;
            this.zkClient = CuratorFrameworkFactory.newClient(sftpConfiguration.zkConnection(),
                    properties.getSessionTimeoutMs(),
                    properties.getConnectionTimeoutMs(),
                    new ExponentialBackoffRetry(1000, 5));
            this.zkClient.start();

            zkNodesPath = properties.getNodesPath() + FILE_SEPARATOR + sftpConfiguration.lockName();
            zkLockPath = properties.getLockPath() + FILE_SEPARATOR + sftpConfiguration.lockName();

            createConfigNodes();

            nodes = watchNodes();
            setWatcher();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void createConfigNodes() {
        log.info("Create nodes start");
        try {
            Stat isAppNodeExist = zkClient.checkExists()
                    .forPath(zkNodesPath);
            if (Objects.isNull(isAppNodeExist)) {
                zkClient.create().creatingParentsIfNeeded()
                        .forPath(zkNodesPath);
            }

            Stat isLockNodeExist = zkClient.checkExists()
                    .forPath(zkLockPath);
            if (Objects.isNull(isLockNodeExist)) {
                zkClient.create().creatingParentsIfNeeded()
                        .forPath(zkLockPath);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("Nodes created");
    }

    public void connect() {
        try {
            selectingAndCreatingNode();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    private void connectToSftp(String nodeNumber, int nodeCount) {
        log.info("connection start");
        String tmpLockPath = zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(zkLockPath + FILE_SEPARATOR + nodeNumber);
        log.info("nodeNumber: " + nodeNumber);
        log.info("nodes in doConnect: " + nodes);
        log.info("lockPath: " + tmpLockPath);

        Session session = null;
        ChannelSftp channelSftp = null;
        try {
            JSch jSch = new JSch();
            session = jSch.getSession(sftpConfiguration.user(), sftpConfiguration.host(), sftpConfiguration.port());
            session.setPassword(sftpConfiguration.password());
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            Map<String, Long> fileNameTimeMap = new HashMap<>();
            findFilesRecursively(sftpConfiguration,
                    channelSftp,
                    sftpConfiguration.remoteFilePath(),
                    fileNameTimeMap,
                    sftpConfiguration.isRecursively());
            log.info("fileNameTimeMap size: " + fileNameTimeMap.size());
            Map<String, Long> filteredMap = fileNameTimeMap.entrySet()
                    .stream()
                    .filter(stringLongEntry -> {
                        String filename = stringLongEntry.getKey();
                        int abs = Math.abs(filename.hashCode() % nodeCount);
                        log.info("filename: " + filename);
                        log.info("hashCode: " + abs);

                        return abs == Integer.parseInt(nodeNumber);
                    })
                    .limit(sftpConfiguration.fileCount())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            for (Map.Entry<String, Long> entry : filteredMap.entrySet()) {
                String path = entry.getKey();
                log.info("File processing: " + path);
                try (InputStream inputStream = channelSftp.get(path);
                     BufferedReader bufferedReader = new BufferedReader(supplyInputStreamReader(Files.getFileExtension(path), inputStream))) {
                    String line;
                    int rowNumber = 0;
                    while ((line = bufferedReader.readLine()) != null) {
                        if (rowNumber >= sftpConfiguration.skipRow()) {
                            String message = new String(line + sftpConfiguration.delimiter() + entry.getValue());
                            System.out.println(message.getBytes(StandardCharsets.UTF_8));
//                            sendPayload(message.getBytes(StandardCharsets.UTF_8));
                        }
                        rowNumber++;
                    }
                    channelSftp.rm(path);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            log.info("connection completed");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (channelSftp != null) {
                channelSftp.exit();
            }
            if (session != null) {
                session.disconnect();
            }
            zkClient.delete()
                    .forPath(tmpLockPath);
        }
    }

    @SneakyThrows
    public void disconnect() {
        isDisconnected = true;
        stopExecutor();
        Stat isAppNodeExist = zkClient.checkExists()
                .forPath(zkNodesPath + FILE_SEPARATOR + nodeNumber);
        if (Objects.nonNull(isAppNodeExist)) {
            zkClient.delete()
                    .forPath(zkNodesPath + FILE_SEPARATOR + nodeNumber);
        }
        Stat isLockNodeExist = zkClient.checkExists()
                .forPath(zkLockPath);
        if (Objects.nonNull(isLockNodeExist)) {
            zkClient.delete()
                    .forPath(zkLockPath + FILE_SEPARATOR + nodeNumber);
        }
        log.info("disconnected");
    }

    private void stopExecutor() {
        executor.shutdown();
        boolean termination;
        try {
            termination = executor.awaitTermination(5000, TimeUnit.MICROSECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        log.info("disconnect executor: " + termination);
    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        if (!isDisconnected) {
            setWatcher();
        }
        log.info("WatchedEvent start: " + event.getType());

        List<String> newNodes = watchNodes();
        if (nodes.size() > newNodes.size() && !newNodes.isEmpty()) {
            Stat nodesStat = zkClient.checkExists()
                    .forPath(zkNodesPath + FILE_SEPARATOR + nodeNumber);
            if (Objects.nonNull(nodesStat)) {
                zkClient.delete()
                        .forPath(zkNodesPath + FILE_SEPARATOR + nodeNumber);
            }
        }
        nodes = watchNodes();
        log.info("watched event nodes: " + nodes);

        log.info("REBALANCING START");
        if (Objects.nonNull(scheduledFuture)) {
            log.info("task is canceled : " + scheduledFuture.cancel(false));
        }

        List<String> lockChildren = zkClient.getChildren()
                .forPath(zkLockPath);
        log.info("WatchedEvent lock nodes: " + lockChildren);

        while (!lockChildren.isEmpty()) {
            lockChildren = zkClient.getChildren()
                    .forPath(zkLockPath);
            log.info("watched event lock nodes: " + lockChildren);
            TimeUnit.SECONDS.sleep(1);
        }

        log.info("isDisconnected: " + isDisconnected);
        if (!isDisconnected) {
            if (!nodes.contains(nodeNumber)) {
                selectingAndCreatingNode();
            }
            if (!nodes.isEmpty()) {
                startExecutor(nodes.size());
            }
        }
        log.info("REBALANCING COMPLETED");
    }

    @SneakyThrows
    private List<String> watchNodes() {
        List<String> children = zkClient.getChildren().forPath(zkNodesPath);
        log.info("nodes: " + children);

        return children;
    }

    @SneakyThrows
    private void setWatcher() {
        zkClient.getChildren()
                .usingWatcher(this)
                .forPath(zkNodesPath);
        log.info("watcher set");
    }

    @SneakyThrows
    public void selectingAndCreatingNode() {
        log.info("node selection start");
        int nextFolderNumber = getNextFolder(zkNodesPath, zkClient);

        Stat isNodeExist = zkClient.checkExists()
                .forPath(zkNodesPath + FILE_SEPARATOR + nextFolderNumber);

        while (Objects.nonNull(isNodeExist)) {
            nextFolderNumber = getNextFolder(zkNodesPath, zkClient);
            isNodeExist = zkClient.checkExists()
                    .forPath(zkNodesPath + FILE_SEPARATOR + nextFolderNumber);
            TimeUnit.SECONDS.sleep(1);
        }

        try {
            String fullPath = zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(zkNodesPath + FILE_SEPARATOR + nextFolderNumber);

            nodeNumber = ZKPaths.getNodeFromPath(fullPath);
            log.info("node full path: " + fullPath);
            log.info("node created: " + nodeNumber);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("node selection finished");
    }

    private static int getNextFolder(String zkPath, CuratorFramework client) throws Exception {
        List<String> children = client.getChildren().forPath(zkPath);
        log.info("children: " + children);

        int nextFolderNumber = 0;
        for (String s : children) {
            if (!String.valueOf(nextFolderNumber).equals(s)) {
                break;
            }
            nextFolderNumber++;
        }

        return nextFolderNumber;
    }

    private void startExecutor(int nodeCount) {
        log.info("executor start");
        synchronized (zkLock) {
            if (delayedStartFutureTask != null && delayedStartFutureTask.isDone()) {
                delayedStartFutureTask.cancel(false);
            }

            delayedStartFutureTask = delayedStartScheduler.schedule(() -> {
                this.scheduledFuture = executor.scheduleAtFixedRate(
                        () -> connectToSftp(nodeNumber, nodeCount),
                        sftpConfiguration.initialDelay(),
                        sftpConfiguration.period(),
                        TimeUnit.SECONDS
                );
            }, properties.getDelayedStartSec(), TimeUnit.SECONDS);
        }
        log.info("executor finish");
    }
}
