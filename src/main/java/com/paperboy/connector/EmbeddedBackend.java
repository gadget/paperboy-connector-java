package com.paperboy.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class EmbeddedBackend implements MessagingBackend {

    private static final Log LOG = LogFactory.getLog(EmbeddedBackend.class);

    private final Map<String, MessageHandler> messageHandlers = new ConcurrentHashMap<>();
    private List<String> embeddedBackendServices = new ArrayList<>();
    private final AtomicInteger embeddedBackendServiceIdx = new AtomicInteger(0);

    private String localAddress;
    private HttpClient httpClient;
    private String embeddedBackendToken;
    private ObjectMapper objectMapper;
    private ScheduledExecutorService executorService;

    public EmbeddedBackend(String embeddedBackendToken) {
        this.embeddedBackendToken = embeddedBackendToken;
        this.executorService = Executors.newScheduledThreadPool(1);
    }

    @Override
    public void close() {
        executorService.shutdown();
    }

    @Override
    public void init() {
        try {
            LOG.info("Initializing embedded backend...");
            httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            try (final DatagramSocket socket = new DatagramSocket()) {
                socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                localAddress = socket.getLocalAddress().getHostAddress();
            }
            objectMapper = new ObjectMapper();
            new ServiceDiscoveryTask().run(); // first we run synchronously, then schedule it
            executorService.schedule(new ServiceDiscoveryTask(), 10, TimeUnit.SECONDS);
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

    public void messageCallback(String topic, Object msg, String providedEmbeddedBackendToken) {
        LOG.info(String.format("Message callback on topic '%s'.", topic));
        if (!this.embeddedBackendToken.equals(providedEmbeddedBackendToken)) {
            throw new IllegalArgumentException("Invalid token for embedded backend!");
        }
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
            String url = service + path;
            LOG.info(String.format("Calling service '%s'.", url));
            HttpRequest post = HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(msg)))
                    .header("Content-Type", "application/json")
                    .header("PaperboyEmbeddedBackendToken", embeddedBackendToken)
                    .uri(URI.create(url))
                    .build();
            httpClient.send(post, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
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

    private class ServiceDiscoveryTask implements Runnable {

        @Override
        public void run() {
            LOG.info("Running service discovery...");
            try {
                List<String> tmp = new ArrayList<>();
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
                    tmp.add("http://" + host + ":" + info.getPort());
                }
                embeddedBackendServices = tmp;
                jmdns.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
