package com.djbx.log.kafka.producer;

import com.djbx.log.kafka.config.Slf4jKafkaConfig;
import com.djbx.log.logback.KafkaAppender;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaSenderHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAppender.class);
    private static volatile KafkaSenderHandler instance;
    private AtomicReference<KafkaProducer<String, String>> kafkaProducer = new AtomicReference<>();
    private final BlockingQueue<ProducerRecord<String, String>> queue;
    private final Slf4jKafkaConfig slf4jKafkaConfig;
    private final AtomicBoolean isConnected = new AtomicBoolean(true);
    private final AtomicInteger failedAttempts = new AtomicInteger(0);
    private final ExecutorService executorService;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private static final int QUEUE_CAPACITY = 1000;
    private static final int BATCH_SIZE = 100;
    private static final long FLUSH_INTERVAL_MS = 3000; //flush 间隔时间
    private static final long POLL_TIMEOUT_MS = 1000; //队列为空时 阻塞等待时间
    private static final int MAX_FAILED_ATTEMPTS = 3;  //失败重连次数
    private static final long RETRY_INTERVAL = 3000;

    private KafkaSenderHandler(Slf4jKafkaConfig slf4jKafkaConfig) {
        this.slf4jKafkaConfig = slf4jKafkaConfig;
        this.producerInit();
        this.queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.executorService = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "KafkaSenderThread");
            t.setDaemon(true);
            return t;
        });
        start();
    }

    private void producerInit() {
        Properties props = new Properties();
        props.put("bootstrap.servers", slf4jKafkaConfig.getBootstrapServers());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("request.timeout.ms", 1000); // 请求超时设置为1秒
        props.put("metadata.max.age.ms", 1000); // 元数据的最大时间（毫秒），1秒
        props.put("max.block.ms", 1000); // 最大阻塞时间设置为1秒
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer.set(new KafkaProducer(props));

        try { // 测试连通
            this.kafkaProducer.get().partitionsFor(slf4jKafkaConfig.getKafkaTopic());
        } catch (Exception e) {
            handleSendFailure();
        }
    }

    public static KafkaSenderHandler getInstance(Slf4jKafkaConfig slf4jKafkaConfig) {
        if (instance == null) {
            synchronized (KafkaSenderHandler.class) {
                if (instance == null) {
                    instance = new KafkaSenderHandler(slf4jKafkaConfig);
                }
            }
        }
        return instance;
    }

    public boolean isConnected() {
        return this.isConnected.get();
    }

    private void start() {
        executorService.submit(this::sendLoop);
    }

    private void sendLoop() {
        List<ProducerRecord<String, String>> batch = new ArrayList<>(BATCH_SIZE);
        long lastFlushTime = System.currentTimeMillis();

        while (isRunning.get()) {
            try {
                if (!this.isConnected.get()) {
                    logger.error("【kafka连接失败】，关闭日志获取！");
                    this.shutdown();
                }

                drainQueue(batch);
                long currentTime = System.currentTimeMillis();
                if (!batch.isEmpty() || currentTime - lastFlushTime > FLUSH_INTERVAL_MS) {
                    sendBatch(batch);
                    kafkaProducer.get().flush();
                    lastFlushTime = currentTime;
                }

                if (batch.isEmpty()) {
                    // 如果批次为空，短暂休眠以减少CPU使用
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                //NOTHING TO DO
            } catch (Exception e) { //未知异常处理
                e.printStackTrace();
                this.shutdown();
                break;
            }
        }
    }

    private void handleSendFailure() {
        if (failedAttempts.incrementAndGet() >= MAX_FAILED_ATTEMPTS) {
            isConnected.set(false);
            return;
        }
        this.reconnect();
    }
    private void reconnect() {
        try {
            Thread.sleep(RETRY_INTERVAL);

            // Create a new producer
            this.producerInit();

            isConnected.set(true);
            failedAttempts.set(0);

        } catch (InterruptedException e) {
            //NOTHING TO DO
        } catch (Exception e) {
            logger.error("Error while trying to reconnect to Kafka of logger: " + e.getMessage());
            failedAttempts.incrementAndGet();
        }
    }

    private void drainQueue(List<ProducerRecord<String, String>> batch) throws InterruptedException {
        batch.clear();
        queue.drainTo(batch, BATCH_SIZE);
        if (batch.isEmpty()) {
            // 如果队列为空，等待一段时间
            ProducerRecord<String, String> record = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (record != null) {
                batch.add(record);
            }
        }
    }

    private void sendBatch(List<ProducerRecord<String, String>> batch) {
        for (ProducerRecord<String, String> record : batch) {
            try {
                kafkaProducer.get().send(record, createCallback());
            } catch (Exception e) {
                System.err.println("Error sending message: " + e.getMessage());
                handleSendFailure();
            }
        }
    }

    private Callback createCallback() {
        return (metadata, exception) -> {
            if (exception != null) {
                handleSendFailure();
            } else {
                failedAttempts.set(0);
                isConnected.set(true);
            }
        };
    }

    public void send(ProducerRecord<String, String> producerRecord) {
        if (!queue.offer(producerRecord)) {
            logger.error("Kafka send queue is full, message dropped");
        }
    }

    public void shutdown() {
        isRunning.set(false);

        try {
            kafkaProducer.get().close();
        } catch (Exception e) {
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
