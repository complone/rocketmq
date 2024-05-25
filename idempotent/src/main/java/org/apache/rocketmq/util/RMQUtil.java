package org.apache.rocketmq.util;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.lang.reflect.Field;
import java.util.*;

public class RMQUtil {
    private static final Set<String> ReservedKeySetRMQ = new HashSet();
    private static final Set<String> ReservedKeySetSystemEnv = new HashSet();

    static {
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_KEYS);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_TAGS);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_RETRY_TOPIC);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_REAL_TOPIC);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_REAL_QUEUE_ID);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_PRODUCER_GROUP);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_MIN_OFFSET);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_MAX_OFFSET);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.TAG);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.KEY);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.MSGID);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.RECONSUMETIMES);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.STARTDELIVERTIME);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.BORNHOST);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.BORNTIMESTAMP);
        ReservedKeySetSystemEnv.add(Message.SystemPropKey.SHARDINGKEY);
    }

    public static Message msgConvert(Message msgRMQ) {
        Message message = new Message();
        if (msgRMQ.getTopic() != null) {
            message.setTopic(msgRMQ.getTopic());
        }

        if (msgRMQ.getKeys() != null) {
            message.setKey(msgRMQ.getKeys());
        }

        if (msgRMQ.getTags() != null) {
            message.setTag(msgRMQ.getTags());
        }

        if (msgRMQ.getBody() != null) {
            message.setBody(msgRMQ.getBody());
        }

        message.setReconsumeTimes(((MessageExt)msgRMQ).getReconsumeTimes());
        message.setBornTimestamp(((MessageExt)msgRMQ).getBornTimestamp());
        message.setBornHost(String.valueOf(((MessageExt)msgRMQ).getBornHost()));
        Map<String, String> properties = msgRMQ.getProperties();
        if (properties != null) {
            Iterator<Map.Entry<String, String>> it = properties.entrySet().iterator();

            while(true) {
                while(it.hasNext()) {
                    Map.Entry<String, String> next = (Map.Entry)it.next();
                    if (!ReservedKeySetRMQ.contains(next.getKey()) && !ReservedKeySetSystemEnv.contains(next.getKey())) {
                        message.putUserProperties((String)next.getKey(), (String)next.getValue());
                    } else {
                        MessageAccessor.putSystemProperties(message, (String)next.getKey(), (String)next.getValue());
                    }
                }

                return message;
            }
        } else {
            return message;
        }
    }


    public static Message msgConvert(Message message) throws MQClientException {
        Message msgRMQ = new Message();
        if (message == null) {
            throw new MQClientException(ResponseCode.NO_MESSAGE, "'message' is null");
        } else {
            if (message.getTopic() != null) {
                msgRMQ.setTopic(message.getTopic());
            }

            if (message.getKey() != null) {
                msgRMQ.setKeys(message.getKey());
            }

            if (message.getTag() != null) {
                msgRMQ.setTags(message.getTag());
            }

            if (message.getStartDeliverTime() > 0L) {
                msgRMQ.putUserProperty("__STARTDELIVERTIME", String.valueOf(message.getStartDeliverTime()));
            }

            if (message.getBody() != null) {
                msgRMQ.setBody(message.getBody());
            }

            if (message.getShardingKey() != null && !message.getShardingKey().isEmpty()) {
                msgRMQ.putUserProperty("__SHARDINGKEY", message.getShardingKey());
            }

            Properties systemProperties = MessageAccessor.getSystemProperties(message);
            if (systemProperties != null) {
                Iterator<Map.Entry<Object, Object>> it = systemProperties.entrySet().iterator();

                while(it.hasNext()) {
                    Map.Entry<Object, Object> next = (Map.Entry)it.next();
                    if (!ReservedKeySetSystemEnv.contains(next.getKey().toString())) {
                        MessageAccessor.putProperty(msgRMQ, next.getKey().toString(), next.getValue().toString());
                    }
                }
            }

            Properties userProperties = new Properties();
            userProperties.putAll(message.getProperties());
            if (userProperties != null) {
                Iterator<Map.Entry<Object, Object>> it = userProperties.entrySet().iterator();

                while(it.hasNext()) {
                    Map.Entry<Object, Object> next = (Map.Entry)it.next();
                    if (!ReservedKeySetRMQ.contains(next.getKey().toString())) {
                        MessageAccessor.putProperty(msgRMQ, next.getKey().toString(), next.getValue().toString());
                    }
                }
            }

            return msgRMQ;
        }
    }

    public static Properties extractProperties(Properties properties) {
        Properties newPro = new Properties();
        Properties inner = null;
        try {
            Field field = Properties.class.getDeclaredField("defaults");
            field.setAccessible(true);
            inner = (Properties) field.get(properties);
        } catch (Exception e) {
        }
        if (inner != null) {
            for (Map.Entry<Object, Object> entry : inner.entrySet()) {
                newPro.setProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }
        for (Map.Entry<Object, Object> entry2 : properties.entrySet()) {
            newPro.setProperty(String.valueOf(entry2.getKey()), String.valueOf(entry2.getValue()));
        }
        return newPro;
    }
}
