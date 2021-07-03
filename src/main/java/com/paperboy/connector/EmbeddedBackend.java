package com.paperboy.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class EmbeddedBackend implements MessagingBackend {

    private static final Log LOG = LogFactory.getLog(EmbeddedBackend.class);

    private final Map<String, MessageHandler> messageHandlers = new ConcurrentHashMap<>();
    private final List<String> embeddedBackendServices = new ArrayList<>();
    private final AtomicInteger embeddedBackendServiceIdx = new AtomicInteger(0);

    private String localAddress;
    private HttpClient httpClient;
    private ObjectMapper objectMapper;

    public static void main(String[] args) {
        new EmbeddedBackend().init();
    }

    @Override
    public void init() {
        try {
            LOG.info("Initializing embedded backend...");
            localAddress = InetAddress.getLocalHost().getHostAddress();
            //httpClient = HttpClientBuilder.create().build();
            objectMapper = new ObjectMapper();

            JmDNS jmdns = JmDNS.create(InetAddress.getLocalHost());
            ServiceInfo[] serviceInfos = jmdns.list("_paperboy-http._tcp.local.", 6000);
            if (serviceInfos.length == 0) {
                throw new IllegalStateException("No embedded backend found!");
            }
            for (ServiceInfo info : serviceInfos) {
                String host = "localhost";
                if (info.getHostAddresses() != null && info.getHostAddresses().length > 0) {
                    host = info.getHostAddresses()[0];
                }
                LOG.info(String.format("Discovered embedded backend instance on paperboy node '%s:%d'.", host, info.getPort()));
                embeddedBackendServices.add("http://" + host + ":" + info.getPort());
            }
            jmdns.close();
        } catch (IOException e) {
            new UncheckedIOException(e);
        }
    }

    @Override
    public void publish(String topic, Object msg) {
        LOG.info(String.format("Publishing message on topic '%s'.", topic));
        callAllServices("/pushMessage/" + topic, msg);
    }

    @Override
    public void listen(String queue, MessageHandler messageHandler) {
        LOG.info(String.format("Listening for messages on topic '%s'.", queue));
        messageHandlers.put(queue, messageHandler);
        Caller caller = new Caller(localAddress, 8080, "/messageCallback/" + queue);
        callService("/subscribeTopic/" + queue, caller);
    }

    public void messageCallback(String topic, Object msg) {
        LOG.info(String.format("Message callback on topic '%s'.", topic));
        if (messageHandlers.containsKey(topic)) {
            LOG.info("Message handler found, calling.");
            messageHandlers.get(topic).handleMessage(topic, msg.toString());
        }
    }

    private void callAllServices(String path, Object msg) {
        for (String service : embeddedBackendServices) {
            callService(service, path, msg);
        }
    }

    private void callService(String path, Object msg) {
        int nextIdx = embeddedBackendServiceIdx.incrementAndGet();
        if (nextIdx >= embeddedBackendServices.size()) {
            embeddedBackendServiceIdx.set(0);
            nextIdx = 0;
        }
        callService(embeddedBackendServices.get(nextIdx), path, msg);
    }

    private void callService(String service, String path, Object msg) {
        try {
            this.httpClient = HttpClientBuilder.create().build();
            String url = service + path;
            LOG.info(String.format("Calling service '%s'.", url));
            HttpPost post = new HttpPost(url);
            HttpEntity entity = new StringEntity(objectMapper.writeValueAsString(msg));
            post.setEntity(entity);
            post.setHeader("Content-Type", "application/json");
            HttpResponse response = httpClient.execute(post);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class Caller {

        private String restHostname;
        private Integer restPort;
        private String restPath;

        Caller(String restHostname, Integer restPort, String restPath) {
            this.restHostname = restHostname;
            this.restPort = restPort;
            this.restPath = restPath;
        }

        public String getRestHostname() {
            return restHostname;
        }

        public void setRestHostname(String restHostname) {
            this.restHostname = restHostname;
        }

        public Integer getRestPort() {
            return restPort;
        }

        public void setRestPort(Integer restPort) {
            this.restPort = restPort;
        }

        public String getRestPath() {
            return restPath;
        }

        public void setRestPath(String restPath) {
            this.restPath = restPath;
        }
    }
}
