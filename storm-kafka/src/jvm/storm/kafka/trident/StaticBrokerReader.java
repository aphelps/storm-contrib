package storm.kafka.trident;

import java.util.HashMap;
import java.util.Map;

import storm.kafka.BrokerData;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;


/**
 * TODO - obsolete this. With leader election the mappings can't be static any more
 */
public class StaticBrokerReader implements IBrokerReader {

    Map<String, BrokerData> brokers = new HashMap<String, BrokerData>();

    public StaticBrokerReader(StaticHosts hosts) {
        for (HostPort hp : hosts.hosts) {
            BrokerData info = new BrokerData(hp.port);
            brokers.put(hp.host, info);
        }
    }

    @Override
    public Map<String, BrokerData> getCurrentBrokers(boolean force) {
        return brokers;
    }

    @Override
    public void close() {
    }
}
