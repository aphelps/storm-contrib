package storm.kafka.trident;

import java.util.Map;

import storm.kafka.BrokerData;
import storm.kafka.DynamicBrokersReader;
import storm.kafka.KafkaConfig.ZkHosts;


public class ZkBrokerReader implements IBrokerReader {

    Map<String, BrokerData> cachedBrokers;
    DynamicBrokersReader reader;
    long lastRefreshTimeMs;
    long refreshMillis;
    
    public ZkBrokerReader(Map conf, String topic, ZkHosts hosts) {
        reader = new DynamicBrokersReader(conf, hosts.brokerZkStr, hosts.brokerZkPath, topic);
        cachedBrokers = reader.getBrokerInfo();
        lastRefreshTimeMs = System.currentTimeMillis();
        refreshMillis = hosts.refreshFreqSecs * 1000L;
    }
    
    @Override
    public Map<String, BrokerData> getCurrentBrokers() {
        long currTime = System.currentTimeMillis();
        if(currTime > lastRefreshTimeMs + refreshMillis) {
            cachedBrokers = reader.getBrokerInfo();
            lastRefreshTimeMs = currTime;
        }
        return cachedBrokers;
    }

    @Override
    public void close() {
        reader.close();
    }    
}
