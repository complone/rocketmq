package org.apache.rocketmq.impl;



import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.admin.Admin;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.DefaultTopAddressing;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.impl.namespace.InstanceUtil;
import org.apache.rocketmq.impl.properties.PropertyKeyConst;
import org.apache.rocketmq.impl.properties.PropertyValueConst;
import org.apache.rocketmq.log.InternalLogger;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.trace.dispatch.AsyncDispatcher;
import org.apache.rocketmq.util.ClientLoggerUtil;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class RMQClientAbstract implements Admin {
    protected static final String WSADDR_INTERNAL = MixAll.getWSAddr();
    protected static final String WSADDR_INTERNET =MixAll.getWSAddr();
    protected static final long WSADDR_INTERNAL_TIMEOUTMILLS = Long.parseLong(System.getProperty("com.############", "3000"));
    protected static final long WSADDR_INTERNET_TIMEOUTMILLS = Long.parseLong(System.getProperty("com.##################", "5000"));
    private static final InternalLogger log = ClientLoggerUtil.getClientLogger();
    protected final Properties properties;
    protected String nameServerAddr;
    protected String proxyAddr;
    protected final String namespaceId;
    protected AccessChannel accessChannel;
    protected final SessionCredentials sessionCredentials = new SessionCredentials();
    protected TraceDispatcher traceDispatcher = null;
    protected final AtomicBoolean started = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "TuxeClient-UpdateNameServerThread");
        }
    });

    protected abstract void updateNameServerAddr(String str);

    protected abstract void updateProxyAddr(String str);

    public RMQClientAbstract(Properties properties) throws MQClientException {
        this.nameServerAddr = NameServerAddressUtils.getNameServerAddresses();
        this.properties = properties;
        this.sessionCredentials.updateContent(properties);
        if (Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.AuthenticationRequired))) {
            if (null == this.sessionCredentials.getAccessKey() || "".equals(this.sessionCredentials.getAccessKey())) {
                throw new MQClientException(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED, "please set access key");
            } else if (null == this.sessionCredentials.getSecretKey() || "".equals(this.sessionCredentials.getSecretKey())) {
                throw new MQClientException(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED, "please set secret key");
            }
        }
        this.accessChannel = AccessChannel.LOCAL;
        this.namespaceId = parseNamespaceId();
        this.nameServerAddr = this.properties.getProperty("NAMESRV_ADDR");
        if (this.nameServerAddr == null) {
            this.proxyAddr = this.properties.getProperty("PROXY_ADDR");
            if (this.proxyAddr == null) {
                this.proxyAddr = fetchProxyAddr();
                if (null == this.proxyAddr) {
                    throw new MQClientException(ResponseCode.NO_PROXY, "Can not find proxy server, May be your network problem.");
                }
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            String psAddrs = RMQClientAbstract.this.fetchProxyAddr();
                            if (psAddrs != null && !RMQClientAbstract.this.proxyAddr.equals(psAddrs)) {
                                RMQClientAbstract.this.proxyAddr = psAddrs;
                                if (RMQClientAbstract.this.isStarted()) {
                                    RMQClientAbstract.this.updateProxyAddr(psAddrs);
                                }
                            }
                        } catch (Exception e) {
                            RMQClientAbstract.log.error("update name server periodically failed.", (Throwable) e);
                        }
                    }
                }, 10000, ExponentialBackOff.DEFAULT_MAX_INTERVAL, TimeUnit.MILLISECONDS);
            }
        }
    }

    public String fetchProxyAddr() throws MQClientException {
        String addr = this.properties.getProperty(PropertyKeyConst.RMQAddr);
        if (addr != null) {
            String netType = this.properties.getProperty(PropertyKeyConst.NetType, PropertyValueConst.IPV4);
            if (netType.equals(PropertyValueConst.IPV4)) {
                addr = addr + "/ipv4";
            } else if (netType.equals(PropertyValueConst.IPV6)) {
                addr = addr + "/ipv6";
            }
            String psAddrs = new DefaultTopAddressing(addr).fetchNSAddr();
            if (psAddrs != null) {
                log.info("connected to user-defined  addr server, {} success, {}", addr, psAddrs);
                return psAddrs;
            }
            throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST, "Can not find proxy with Addr " + addr);
        }
        String psAddrs2 = new DefaultTopAddressing(WSADDR_INTERNAL).fetchNSAddr(false, WSADDR_INTERNET_TIMEOUTMILLS);
        if (psAddrs2 != null) {
            log.info("connected to internal server, {} success, {}", WSADDR_INTERNAL, psAddrs2);
            return psAddrs2;
        }
        String psAddrs3 = new DefaultTopAddressing(WSADDR_INTERNET).fetchNSAddr(false, WSADDR_INTERNET_TIMEOUTMILLS);
        if (psAddrs3 != null) {
            log.info("connected to internet server, {} success, {}", WSADDR_INTERNET, psAddrs3);
        }
        return psAddrs3;
    }

    public String getNameServerAddr() {
        return this.nameServerAddr;
    }

    public String getProxyAddr() {
        return this.proxyAddr;
    }

    protected String buildInstanceName() {
        return Integer.toString(UtilAll.getPid()) + "#" + (this.nameServerAddr == null ? this.proxyAddr.hashCode() : this.nameServerAddr.hashCode()) + "#" + (this.sessionCredentials.getAccessKey() != null ? Integer.valueOf(this.sessionCredentials.getAccessKey().hashCode()) : this.sessionCredentials.getAccessKey()) + "#" + System.nanoTime();
    }

    private String parseNamespaceId() {
        String namespaceId = null;
        String namespaceFromProperty = this.properties.getProperty("INSTANCE_ID", null);
        if (StringUtils.isNotEmpty(namespaceFromProperty)) {
            namespaceId = namespaceFromProperty;
            log.info("User specify namespaceId by property: {}.", namespaceId);
        }
        if (StringUtils.isBlank(namespaceId)) {
            return "";
        }
        return namespaceId;
    }

    protected String getNamespace() {
        return InstanceUtil.isIndependentNaming(this.namespaceId) ? this.namespaceId : "";
    }

    protected void checkONSProducerServiceState(DefaultMQProducerImpl producer) throws MQClientException {
        switch (producer.getServiceState()) {
            case CREATE_JUST:
                throw new MQClientException(ResponseCode.CREATE_JUST, String.format("You do not have start the producer[" + UtilAll.getPid() + "], %s", producer.getServiceState()));
            case SHUTDOWN_ALREADY:
                throw new MQClientException(ResponseCode.SHUTDOWN_ALREADY, String.format("Your producer has been shut down, %s", producer.getServiceState()));
            case START_FAILED:
                throw new MQClientException(ResponseCode.START_FAILED,String.format("When you start your service throws an exception, %s", producer.getServiceState()));
            case RUNNING:
            default:
        }
    }

    @Override
    public void start() {
        if (null != this.traceDispatcher) {
            try {
                this.traceDispatcher.start(this.nameServerAddr, this.accessChannel);
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", (Throwable) e);
            }
        }
    }

    @Override 
    public void updateCredential(Properties credentialProperties) throws MQClientException {
        if (null == credentialProperties.getProperty("AccessKey") || "".equals(credentialProperties.getProperty("AccessKey"))) {
            throw new MQClientException(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED, "update credential failed. please set access key.");
        } else if (null == credentialProperties.getProperty("SecretKey") || "".equals(credentialProperties.getProperty("SecretKey"))) {
            throw new MQClientException(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED, "update credential failed. please set secret key");
        } else {
            this.sessionCredentials.updateContent(credentialProperties);
        }
    }

    @Override 
    public void shutdown() {
        if (null != this.traceDispatcher) {
            this.traceDispatcher.shutdown();
        }
        this.scheduledExecutorService.shutdown();
    }

    @Override 
    public boolean isStarted() {
        return this.started.get();
    }

    @Override 
    public boolean isClosed() {
        return !isStarted();
    }
}
