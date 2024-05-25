package org.apache.rocketmq.admin;

import org.apache.rocketmq.client.exception.MQClientException;

import java.util.Properties;

public interface Admin {
    boolean isStarted();

    boolean isClosed();

    void start();

    void updateCredential(Properties properties) throws MQClientException;

    void shutdown();
}
