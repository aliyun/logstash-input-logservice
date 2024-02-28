package com.aliyun.log.logstash;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static javax.management.timer.Timer.ONE_SECOND;


public class LogHubStarter {

    private ClientWorker worker;
    private final AtomicBoolean isStop = new AtomicBoolean(false);

    public void startWorker(String endpoint, String accessId, String accessKey,
                            String project, String logstore,
                            String consumerGroup, String consumer, String position,
                            int checkpointSecond, boolean includeMeta,
                            BlockingQueue<Map<String, String>> queueCache,
                            String proxyHost, int proxyPort, String proxyUsername, String proxyPassword,
                            String proxyDomain, String proxyWorkstation,
                            int fetchIntervalMillis, String query
    ) throws LogHubClientWorkerException {
        if (worker != null) {
            throw new IllegalStateException("worker has already started");
        }
        // 第二个参数是消费者名称，同一个消费组下面的消费者名称必须不同，可以使用相同的消费组名称，
        // 不同的消费者名称在多台机器上启动多个进程，来均衡消费一个Logstore，这个时候消费者名称可以使用机器ip来区分。
        if (consumer == null) {
            throw new IllegalArgumentException("consumer is null");
        }
        LogHubConfig config;
        consumer += "_" + getRandomString(5);
        if ("begin".equals(position)) {
            config = new LogHubConfig(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey, LogHubConfig.ConsumePosition.BEGIN_CURSOR);
        } else if ("end".equals(position)) {
            config = new LogHubConfig(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey, LogHubConfig.ConsumePosition.END_CURSOR);
        } else {
            int time = (int) (Timestamp.valueOf(position).getTime() / ONE_SECOND);
            config = new LogHubConfig(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey, time);
        }
        if (proxyHost != null && !proxyHost.isEmpty()) {
            config.setProxyHost(proxyHost);
        }
        if (proxyPort != 0) {
            config.setProxyPort(proxyPort);
        }
        if (proxyUsername != null && !proxyUsername.isEmpty()) {
            config.setProxyUsername(proxyUsername);
        }
        if (proxyPassword != null && !proxyPassword.isEmpty()) {
            config.setProxyPassword(proxyPassword);
        }
        if (proxyDomain != null && !proxyDomain.isEmpty()) {
            config.setProxyDomain(proxyDomain);
        }
        if (proxyWorkstation != null && !proxyWorkstation.isEmpty()) {
            config.setProxyWorkstation(proxyWorkstation);
        }
        if (fetchIntervalMillis > 0) {
            config.setFetchIntervalMillis(fetchIntervalMillis);
        }
        if (query != null && !query.isEmpty()) {
            config.setQuery(query);
        }
        worker = new ClientWorker(
                new LogstashLogHubProcessorFactory(checkpointSecond, includeMeta, consumerGroup + "/" + consumer, queueCache, isStop),
                config);
        Thread thread = new Thread(worker);
        thread.start();
        isStop.set(false);
    }

    public void stopWorker() {
        if (worker != null) {
            worker.shutdown();
        }
        isStop.set(true);
    }

    public static String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str.length());
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}