package org.apache.rocketmq.trace.impl;


import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.namesrv.DefaultTopAddressing;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.impl.properties.PropertyKeyConst;
import org.apache.rocketmq.trace.common.RMQTraceConstants;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TraceProducerFactory {
    private static Map<String, Object> dispatcherTable = new ConcurrentHashMap();
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private DefaultMQProducer traceProducer;

    public DefaultMQProducer getTraceDispatcherProducer(Properties properties, SessionCredentials sessionCredentials) {
        if (this.traceProducer == null) {
            Properties sessionProperties = new Properties();
            if (Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.AuthenticationRequired))) {
                String accessKey = properties.getProperty("AccessKey");
                String secretKey = properties.getProperty("SecretKey");
                sessionProperties.put("AccessKey", accessKey);
                sessionProperties.put("SecretKey", secretKey);
                sessionCredentials.updateContent(sessionProperties);
                this.traceProducer = new DefaultMQProducer(new AclClientRPCHook(sessionCredentials));
                this.traceProducer.setProducerGroup(accessKey + RMQTraceConstants.groupName);
            } else {
                this.traceProducer = new DefaultMQProducer(RMQTraceConstants.groupName);
            }
            this.traceProducer.setSendMsgTimeout(100);
            this.traceProducer.setInstanceName(properties.getProperty("InstanceName", String.valueOf(System.currentTimeMillis())));
            String nameSrv = properties.getProperty("NAMESRV_ADDR");
            String proxySrv = properties.getProperty("PROXY_ADDR");
            if (nameSrv == null && null == proxySrv) {
                this.traceProducer.setProxyAddr(new DefaultTopAddressing(properties.getProperty(RMQTraceConstants.ADDRSRV_URL)).fetchNSAddr());
            } else if (null == nameSrv) {
                this.traceProducer.setProxyAddr(proxySrv);
            } else {
                this.traceProducer.setNamesrvAddr(nameSrv);
            }
            this.traceProducer.setVipChannelEnabled(false);
            this.traceProducer.setMaxMessageSize(Integer.parseInt(properties.getProperty(RMQTraceConstants.MaxMsgSize, "128000")) - 10000);
        }
        return this.traceProducer;
    }

    public void registerTraceDispatcher(String dispatcherId) throws MQClientException {
        dispatcherTable.put(dispatcherId, new Object());
        if (this.traceProducer != null && this.isStarted.compareAndSet(false, true)) {
            this.traceProducer.start();
        }
    }

    public void unregisterTraceDispatcher(String dispatcherId) {
        dispatcherTable.remove(dispatcherId);
        if (dispatcherTable.isEmpty() && this.traceProducer != null && this.isStarted.get()) {
            this.traceProducer.shutdown();
        }
    }
}
