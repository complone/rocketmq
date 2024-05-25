package org.apache.rocketmq.transaction;


import org.apache.rocketmq.admin.Admin;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;

public interface TransactionProducer extends Admin {
    @Override 
    void start();

    @Override 
    void shutdown();

    @Deprecated
    SendResult send(Message message, LocalTransactionExecuter localTransactionExecuter, Object obj) throws MQClientException;

    SendResult send(Message message, Object obj) throws MQClientException;

    void setTransactionListener(TransactionListener transactionListener);
}
