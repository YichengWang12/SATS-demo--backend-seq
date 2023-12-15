package com.ethan.bean;

import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MemoryDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.StoreEngineOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.listener.ChannelListener;
import com.alipay.sofa.rpc.transport.AbstractChannel;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.vertx.core.Vertx;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import thirdpart.codec.IByteCodec;
import thirdpart.fetchserv.IFetchService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Timer;

@Log4j2
@ToString
@RequiredArgsConstructor
public class SeqConfig {
    private String dataPath;

    private String serveUrl;

    private String serverList;

    @NonNull
    private String fileName;

    public void startup() throws Exception{
        //1.read config file
        initConfig();

        //2.initialize kv store cluster
        startSeqDbCluster();

        //3.start broadcast
        startMultiCast();

        //4.initialize gateway connection
        startupFetch();
    }

    ////////////////////////////////////////broadcast/////////////////////

    @Getter
    private String multicastIp;

    @Getter
    private int multicastPort;

    @Getter
    private DatagramSocket multicastSender;
    private void startMultiCast(){
        multicastSender = Vertx.vertx().createDatagramSocket(new DatagramSocketOptions());
    }
    ////////////////////////////////////////fetch////////////////////////

    private String fetchUrls;

    @ToString.Exclude
    @Getter
    private Map<String, IFetchService> fetchServiceMap = Maps.newConcurrentMap();

    @NonNull
    @ToString.Exclude
    @Getter
    private IByteCodec codec;

    @RequiredArgsConstructor
    private class FetchChannelListener implements ChannelListener{
        @NonNull
        private ConsumerConfig<IFetchService> config;

        @Override
        public void onConnected(AbstractChannel abstractChannel) {
            String remoteAddr = abstractChannel.remoteAddress().toString();
            log.info("connected to gateway: {}", remoteAddr);
            fetchServiceMap.put(remoteAddr,config.refer());
        }

        @Override
        public void onDisconnected(AbstractChannel abstractChannel) {
            String remoteAddr = abstractChannel.remoteAddress().toString();
            log.info("disconnected to gateway: {}", remoteAddr);
            fetchServiceMap.remove(remoteAddr);
        }
    }
    //1.from where to fetch
    //2.method of communication
    private void startupFetch() {
        //1. establish all gateway connections
        String[] urls = fetchUrls.split(";");
        for (String url : urls){
            ConsumerConfig<IFetchService> consumerConfig = new ConsumerConfig<IFetchService>()
                    .setInterfaceId(IFetchService.class.getName())
                    .setProtocol("bolt")
                    .setTimeout(5000)
                    .setDirectUrl(url);
            consumerConfig.setOnConnect(Lists.newArrayList(new FetchChannelListener(consumerConfig)));
            fetchServiceMap.put(url,consumerConfig.refer());
        }
        //2. fetch data regularly
        new Timer().schedule(new FetchTask(this),5000,1000);
    }
////////////////////////////////////////
    @Getter
    private Node node;
    //start kv store
    private void startSeqDbCluster() {
        final PlacementDriverOptions pdOptions = PlacementDriverOptionsConfigured.newConfigured()
                .withFake(true)
                .config();

        String[] split = serveUrl.split(":");
        final StoreEngineOptions storeOptions = StoreEngineOptionsConfigured.newConfigured()
                .withStorageType(StorageType.Memory)
                .withMemoryDBOptions(MemoryDBOptionsConfigured.newConfigured().config())
                .withRaftDataPath(dataPath)
                .withServerAddress(new Endpoint(split[0], Integer.parseInt(split[1])))
                .config();

        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured()
                .withInitialServerList(serverList)
                .withStoreEngineOptions(storeOptions)
                .withPlacementDriverOptions(pdOptions)
                .config();

        node = new Node(opts);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        log.info("start seq node success on port: {}",split[1]);
    }

    private void initConfig() throws IOException {
        Properties properties = new Properties();
        log.info(SeqConfig.class.getResourceAsStream("/" + fileName));
        properties.load(SeqConfig.class.getResourceAsStream("/" + fileName));

        dataPath = properties.getProperty("datapath");
        serveUrl = properties.getProperty("serveurl");
        serverList = properties.getProperty("serverlist");
        fetchUrls = properties.getProperty("fetchurls");
        multicastIp = properties.getProperty("multicastip");
        multicastPort = Integer.parseInt(properties.getProperty("multicastport"));

        log.info("read config : {}", this);


    }

}
