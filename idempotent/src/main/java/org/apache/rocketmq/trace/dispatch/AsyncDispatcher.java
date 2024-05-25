package org.apache.rocketmq.trace.dispatch;


import org.apache.rocketmq.client.exception.MQClientException;

import java.io.IOException;

public interface AsyncDispatcher {
    void start() throws MQClientException;

    boolean append(Object obj);

    void flush() throws IOException;

    void shutdown();
}
