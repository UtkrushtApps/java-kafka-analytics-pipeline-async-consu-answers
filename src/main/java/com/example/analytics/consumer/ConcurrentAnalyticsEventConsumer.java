package com.example.analytics.consumer;

import com.example.analytics.model.AnalyticsEvent;
import com.example.analytics.service.AnalyticsEventHandler;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ConcurrentAnalyticsEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentAnalyticsEventConsumer.class);

    @Value("${kafka.consumer.topic}")
    private String topic;

    private final Map<String, Object> consumerConfigs;
    private final AnalyticsEventHandler eventHandler;
    private final MeterRegistry meterRegistry;

    private KafkaConsumer<String, AnalyticsEvent> consumer;
    private ExecutorService workerPool;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private static final int NUM_WORKERS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int POLL_TIMEOUT_MS = 500;
    private static final int COMMIT_INTERVAL = 1000; // milliseconds

    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new ConcurrentHashMap<>();

    public ConcurrentAnalyticsEventConsumer(Map<String,Object> consumerConfigs, AnalyticsEventHandler eventHandler, MeterRegistry meterRegistry) {
        this.consumerConfigs = consumerConfigs;
        this.eventHandler = eventHandler;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void start() {
        this.consumer = new KafkaConsumer<>(consumerConfigs);
        consumer.subscribe(Collections.singletonList(topic));
        workerPool = Executors.newFixedThreadPool(NUM_WORKERS);
        Thread mainLoop = new Thread(this::pollLoop, "analytics-consumer-main-loop");
        mainLoop.start();
        logger.info("Started concurrent Kafka consumer with {} worker threads for topic {}", NUM_WORKERS, topic);
    }

    public void pollLoop() {
        long lastCommitTime = System.currentTimeMillis();
        while (running.get()) {
            try {
                ConsumerRecords<String, AnalyticsEvent> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                if (!records.isEmpty()) {
                    meterRegistry.counter("analytics_event_kafka_records", "type", "polled").increment(records.count());
                    CountDownLatch latch = new CountDownLatch(records.count());
                    for (ConsumerRecord<String, AnalyticsEvent> record : records) {
                        workerPool.submit(() -> {
                            try {
                                eventHandler.handleEvent(record.value());
                                // On success, record offset for at-least-once
                                offsetsToCommit.put(
                                        new TopicPartition(record.topic(), record.partition()),
                                        new OffsetAndMetadata(record.offset() + 1)
                                );
                                meterRegistry.counter("analytics_event_kafka_records", "type", "processed").increment();
                            } catch (Exception e) {
                                meterRegistry.counter("analytics_event_kafka_records", "type", "failed").increment();
                                logger.error("Error processing event: {}", e.getMessage(), e);
                            } finally {
                                latch.countDown();
                            }
                        });
                    }
                    // Wait for all records in this poll to finish
                    latch.await();
                }

                long now = System.currentTimeMillis();
                if (now - lastCommitTime > COMMIT_INTERVAL && !offsetsToCommit.isEmpty()) {
                    commitOffsetsAsync();
                    lastCommitTime = now;
                }

                // Lag monitoring
                for (TopicPartition tp : consumer.assignment()) {
                    long position = consumer.position(tp);
                    long endOffset = consumer.endOffsets(List.of(tp)).get(tp);
                    long lag = endOffset - position;
                    meterRegistry.gauge("analytics_event_kafka_consumer_lag", List.of(), lag);
                }

            } catch (WakeupException e) {
                if (!running.get()) break;
            } catch (Exception ex) {
                logger.error("Consumer loop error: {}", ex.getMessage(), ex);
                meterRegistry.counter("analytics_event_kafka_errors").increment();
            }
        }
        shutdownConsumer();
    }

    private void commitOffsetsAsync() {
        if (offsetsToCommit.isEmpty()) return;
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>(offsetsToCommit);
        consumer.commitAsync(toCommit, (offsets, exception) -> {
            if (exception != null) {
                logger.error("Offset commit failed: {}", exception.getMessage(), exception);
                meterRegistry.counter("analytics_event_kafka_commit_failures").increment();
            }
        });
        offsetsToCommit.keySet().removeAll(toCommit.keySet());
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        if (workerPool != null) {
            workerPool.shutdown();
            try {
                if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    workerPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                workerPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void shutdownConsumer() {
        try {
            if (!offsetsToCommit.isEmpty() && consumer != null) {
                commitOffsetsAsync();
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            logger.info("Kafka consumer closed.");
        }
    }
}
