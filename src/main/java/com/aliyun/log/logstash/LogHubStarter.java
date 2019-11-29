package com.aliyun.log.logstash;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.apache.log4j.Logger;

import java.sql.Timestamp;

import static javax.management.timer.Timer.ONE_SECOND;


public class LogHubStarter {
    private static Logger logger = Logger.getLogger(LogHubStarter.class);

    //public static void startWorker(ILogHubProcessor processor,
    public static void startWorker(String endpoint, String accessId, String accessKey,
                                   String project, String logstore,
                                   String consumerGroup, String consumer, String position, long checkpointSecond, boolean includeMeta, int queueSize) throws LogHubClientWorkerException, InterruptedException {
        // 第二个参数是消费者名称，同一个消费组下面的消费者名称必须不同，可以使用相同的消费组名称，
        // 不同的消费者名称在多台机器上启动多个进程，来均衡消费一个Logstore，这个时候消费者名称可以使用机器ip来区分。
        // 第9个参数（maxFetchLogGroupSize）是每次从服务端获取的LogGroup数目，使用默认值即可，如有调整请注意取值范围(0,1000]
        LogHubConfig config;
        if ("begin".equals(position)) {
            config = new LogHubConfig(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey, LogHubConfig.ConsumePosition.BEGIN_CURSOR);
        } else if ("end".equals(position)) {
            config = new LogHubConfig(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey, LogHubConfig.ConsumePosition.END_CURSOR);
        } else {
            int time = (int) (Timestamp.valueOf(position).getTime() / ONE_SECOND);
            config = new LogHubConfig(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey, time);
        }
        //ClientWorker worker = new ClientWorker(new LogstashLogHubProcessorFactory(processor), config);
        ClientWorker worker = new ClientWorker(new LogstashLogHubProcessorFactory(queueSize), config);
        Thread thread = new Thread(worker);
        //Thread运行之后，Client Worker会自动运行，ClientWorker扩展了Runnable接口。
        thread.start();
        //thread.join();
    }
}
