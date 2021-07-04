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
    private ScheduledExecutorService serviceDiscoveryExecutor;
    private ScheduledExecutorService listenerExecutor;

    public EmbeddedBackend(String embeddedBackendToken) {
        this.embeddedBackendToken = embeddedBackendToken;
        this.serviceDiscoveryExecutor = Executors.newScheduledThreadPool(1);
        this.listenerExecutor = Executors.newScheduledThreadPool(1);
    }

    @Override
    public void close() {
        serviceDiscoveryExecutor.shutdown();
        listenerExecutor.shutdown();
    }

    @Override
    public void init() {
        try {
            LOG.info("Initializing embedded backend...");
            httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            try (final DatagramSocket socket = new DatagramSocket()) {
                socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                // we need a real local address for callbacks (url pointing to a rest endpoint on the webapp is sent to the embedded backend for callbacks)
                // should be wired onto this.messageCallback(topic, msg, providedEmbeddedBackendToken)
                localAddress = socket.getLocalAddress().getHostAddress();
            }
            objectMapper = new ObjectMapper();
            new ServiceDiscoveryTask().run(); // first we run synchronously, then schedule service discovery
            serviceDiscoveryExecutor.scheduleWithFixedDelay(new ServiceDiscoveryTask(), 10, 10, TimeUnit.SECONDS);
        } catch (IOException e) {
            new UncheckedIOException(e);
        }
    }

    @Override
    public void publish(String topic, Object msg) {
        LOG.info(String.format("Publishing message on topic '%s'.", topic));
        // writes are sent to all nodes
        callAllServices("/pushMessage/" + topic, msg);
    }

    @Override
    public void listen(String queue, MessageHandler messageHandler) {
        LOG.info(String.format("Listening for messages on topic '%s'.", queue));
        messageHandlers.put(queue, messageHandler);
        Caller caller = new Caller(localAddress, 8080, "/messageCallback/" + queue); // for callback
        try {
            String service = nextService(); // pick a service from the pool in a round-robin fashion
            String instanceId = instanceIdFor(service); // instanceId is unique to each embedded node
            callService(service, "/subscribeTopic/" + queue, caller);
            // scheduling listener task so that in case of the embedded node fails we re-register on an alive node
            listenerExecutor.scheduleWithFixedDelay(new ListenerTask(queue, caller, service, instanceId), 2, 2, TimeUnit.SECONDS);
        } catch (EmbeddedInstanceRemoteException e) {
            LOG.error(e);
        }
    }

    /**
     * The embedded backend calls the application back when a message is received on a topic it is listening on.
     *
     * @param topic
     * @param msg
     * @param providedEmbeddedBackendToken
     */
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
            try {
                callService(service, path, msg);
            } catch (EmbeddedInstanceRemoteException e) {
                LOG.error(e);
            }
        }
    }

    private String nextService() {
        int nextIdx = embeddedBackendServiceIdx.incrementAndGet();
        if (nextIdx >= embeddedBackendServices.size()) {
            embeddedBackendServiceIdx.set(0);
            nextIdx = 0;
        }
        return embeddedBackendServices.get(nextIdx);
    }

    private String instanceIdFor(String service) throws EmbeddedInstanceRemoteException {
        try {
            HttpRequest post = HttpRequest.newBuilder()
                    .GET()
                    .header("PaperboyEmbeddedBackendToken", embeddedBackendToken)
                    .uri(URI.create(service + "/instance"))
                    .build();
            HttpResponse response = httpClient.send(post, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String instanceId = response.body().toString();
                LOG.info(String.format("Embedded service '%s' has instance id: '%s'.", service, instanceId));
                return instanceId;
            }
            throw new EmbeddedInstanceRemoteException(String.format("Communication error with service: '%s'!", service));
        } catch (IOException e) {
            throw new EmbeddedInstanceRemoteException(e);
        } catch (InterruptedException e) {
            throw new EmbeddedInstanceRemoteException(e);
        }
    }

    private void callService(String service, String path, Object msg) throws EmbeddedInstanceRemoteException {
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
            throw new EmbeddedInstanceRemoteException(e);
        } catch (InterruptedException e) {
            throw new EmbeddedInstanceRemoteException(e);
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
                JmDNS jmdns = JmDNS.create(InetAddress.getLocalHost());
                ServiceInfo[] serviceInfos = jmdns.list("_paperboy-http._tcp.local.", 6000);
                if (serviceInfos.length == 0) {
                    LOG.error("No embedded backend found!");
                } else {
                    List<String> tmp = new ArrayList<>();
                    for (ServiceInfo info : serviceInfos) {
                        String host = "localhost";
                        if (info.getHostAddresses() != null && info.getHostAddresses().length > 0) {
                            host = info.getHostAddresses()[0];
                        }
                        LOG.info(String.format("Discovered embedded backend instance on paperboy node '%s:%d'.", host, info.getPort()));
                        tmp.add("http://" + host + ":" + info.getPort());
                    }
                    embeddedBackendServices = tmp;
                }
                jmdns.close();
            } catch (IOException e) {
                LOG.error(e);
            }
        }
    }

    private class ListenerTask implements Runnable {

        private final String queue;
        private final Caller caller;
        private volatile String service;
        private volatile String instanceId;

        public ListenerTask(String queue, Caller caller, String service, String instanceId) {
            this.queue = queue;
            this.caller = caller;
            this.service = service;
            this.instanceId = instanceId;
        }

        @Override
        public void run() {
            try {
                String currentInstanceId = instanceIdFor(this.service);
                if (!this.instanceId.equals(currentInstanceId)) {
                    // in case an embedded service with the same name exists but it's actually a new instance
                    LOG.info(String.format("Embedded service '%s' has new instance id '%s' -> '%s'.", service, instanceId, currentInstanceId));
                    this.instanceId = currentInstanceId;
                    callService(this.service, "/subscribeTopic/" + queue, caller);
                }
            } catch (EmbeddedInstanceRemoteException e) {
                // in case the embedded instance we currently listing on has failed
                String currentService = nextService(); // we try picking another one
                LOG.info(String.format("Switching from unusable embedded service '%s' -> '%s'.", service, currentService));
                this.service = currentService; // switching onto the new one
                this.instanceId = "N/A";
                try {
                    String currentInstanceId = instanceIdFor(currentService); // instanceId for the new one
                    this.instanceId = currentInstanceId;
                    callService(this.service, "/subscribeTopic/" + queue, caller);
                } catch (EmbeddedInstanceRemoteException ee) {
                    // in case the new one is also failing (in the next iteration we will try picking a new one again...and again...)
                    LOG.error(ee);
                }
            }
        }
    }

}