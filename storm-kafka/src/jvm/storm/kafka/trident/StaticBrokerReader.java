package storm.kafka.trident;

import java.util.HashMap;
import java.util.Map;

import storm.kafka.BrokerData;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;


public class StaticBrokerReader implements IBrokerReader {

    Map<String, BrokerData> brokers = new HashMap<String, BrokerData>();
    
    public StaticBrokerReader(StaticHosts hosts) {
        for(HostPort hp: hosts.hosts) {
            BrokerData info = new BrokerData(hp.port);
          //  TODO - change to let broker configuration contain the partitions on each broker
            brokers.put(hp.host, info);
        }
    }
    
    @Override
    public Map<String, BrokerData> getCurrentBrokers() {
        return brokers;
    }

    @Override
    public void close() {
    }
}
