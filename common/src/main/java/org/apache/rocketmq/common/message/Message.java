/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.message;

import java.io.Serializable;
import java.util.*;

public class Message implements Serializable {
    private static final long serialVersionUID = 8445773977080406428L;

    private String topic;
    Properties systemProperties;
    private int flag;
    private Map<String, String> properties;
    private Properties userProperties;
    private byte[] body;
    private String transactionId;

    public static class SystemPropKey {
        public static final String TAG = "__TAG";
        public static final String KEY = "__KEY";
        public static final String MSGID = "__MSGID";
        public static final String SHARDINGKEY = "__SHARDINGKEY";
        public static final String RECONSUMETIMES = "__RECONSUMETIMES";
        public static final String BORNTIMESTAMP = "__BORNTIMESTAMP";
        public static final String BORNHOST = "__BORNHOST";
        public static final String STARTDELIVERTIME = "__STARTDELIVERTIME";
    }

    public Message() {
    }

    public Message(String topic, byte[] body) {
        this(topic, "", "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
        this.topic = topic;
        this.flag = flag;
        this.body = body;

        if (tags != null && tags.length() > 0) {
            this.setTags(tags);
        }

        if (keys != null && keys.length() > 0) {
            this.setKeys(keys);
        }

        this.setWaitStoreMsgOK(waitStoreMsgOK);
    }

    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, byte[] body) {
        this(topic, tags, keys, 0, body, true);
    }

    public void setKeys(String keys) {
        this.putProperty(MessageConst.PROPERTY_KEYS, keys);
    }

    void putProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        this.properties.put(name, value);
    }

    void clearProperty(final String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }

    public void putUserProperty(final String name, final String value) {
        if (MessageConst.STRING_HASH_SET.contains(name)) {
            throw new RuntimeException(String.format(
                "The Property<%s> is used by system, input another please", name));
        }

        if (value == null || value.trim().isEmpty()
            || name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "The name or value of property can not be null or blank string!"
            );
        }

        this.putProperty(name, value);
    }

    public void putUserProperties(String key, String value) {
        if (null == this.userProperties) {
            this.userProperties = new Properties();
        }
        if (key != null && value != null) {
            this.userProperties.put(key, value);
        }
    }

    public String getUserProperty(final String name) {
        return this.getProperty(name);
    }

    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        return this.properties.get(name);
    }

    public void setTag(String tag) {
        putSystemProperties(SystemPropKey.TAG, tag);
    }


    public String getKey() {
        return getSystemProperties(SystemPropKey.KEY);
    }

    public void setKey(String key) {
        putSystemProperties(SystemPropKey.KEY, key);
    }

    public String getMsgID() {
        return getSystemProperties(SystemPropKey.MSGID);
    }

    public void setMsgID(String msgid) {
        putSystemProperties(SystemPropKey.MSGID, msgid);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return this.getProperty(MessageConst.PROPERTY_TAGS);
    }

    public void setTags(String tags) {
        this.putProperty(MessageConst.PROPERTY_TAGS, tags);
    }

    public String getKeys() {
        return this.getProperty(MessageConst.PROPERTY_KEYS);
    }

    public String getTag() {
        return getSystemProperties(SystemPropKey.TAG);
    }

    public String getShardingKey() {
        String pro = getSystemProperties(SystemPropKey.SHARDINGKEY);
        return pro == null ? "" : pro;
    }

    public String getSystemProperties(String key) {
        if (null != this.systemProperties) {
            return this.systemProperties.getProperty(key);
        }
        return null;
    }

    public void putSystemProperties(String key, String value) {
        if (null == this.systemProperties) {
            this.systemProperties = new Properties();
        }
        if (key != null && value != null) {
            this.systemProperties.put(key, value);
        }
    }

    public void setReconsumeTimes(int value) {
        putSystemProperties(SystemPropKey.RECONSUMETIMES, String.valueOf(value));
    }

    public long getStartDeliverTime() {
        String pro = getSystemProperties(SystemPropKey.STARTDELIVERTIME);
        if (pro != null) {
            return Long.parseLong(pro);
        }
        return 0;
    }

    public long getBornTimestamp() {
        String pro = getSystemProperties(SystemPropKey.BORNTIMESTAMP);
        if (pro != null) {
            return Long.parseLong(pro);
        }
        return 0;
    }

    public void setBornTimestamp(long value) {
        putSystemProperties(SystemPropKey.BORNTIMESTAMP, String.valueOf(value));
    }

    public String getBornHost() {
        String pro = getSystemProperties(SystemPropKey.BORNHOST);
        return pro == null ? "" : pro;
    }

    public void setBornHost(String value) {
        putSystemProperties(SystemPropKey.BORNHOST, value);
    }

    public void setKeys(Collection<String> keyCollection) {
        String keys = String.join(MessageConst.KEY_SEPARATOR, keyCollection);

        this.setKeys(keys);
    }

    public int getDelayTimeLevel() {
        String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (t != null) {
            return Integer.parseInt(t);
        }

        return 0;
    }

    public void setDelayTimeLevel(int level) {
        this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
    }

    public boolean isWaitStoreMsgOK() {
        String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        if (null == result) {
            return true;
        }

        return Boolean.parseBoolean(result);
    }

    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
    }

    public void setInstanceId(String instanceId) {
        this.putProperty(MessageConst.PROPERTY_INSTANCE_ID, instanceId);
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getBuyerId() {
        return getProperty(MessageConst.PROPERTY_BUYER_ID);
    }

    public void setBuyerId(String buyerId) {
        putProperty(MessageConst.PROPERTY_BUYER_ID, buyerId);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return "Message{" +
            "topic='" + topic + '\'' +
            ", flag=" + flag +
            ", properties=" + properties +
            ", body=" + Arrays.toString(body) +
            ", transactionId='" + transactionId + '\'' +
            '}';
    }

    public void setDelayTimeSec(long sec) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC, String.valueOf(sec));
    }

    public long getDelayTimeSec() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    public void setDelayTimeMs(long timeMs) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_MS, String.valueOf(timeMs));
    }

    public long getDelayTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    public void setDeliverTimeMs(long timeMs) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(timeMs));
    }

    public long getDeliverTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }
}
