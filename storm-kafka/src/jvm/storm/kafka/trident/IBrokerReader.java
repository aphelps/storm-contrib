package storm.kafka.trident;

import storm.kafka.BrokerData;

import java.util.Map;


public interface IBrokerReader {    
    /**
     * Map of host to [port, numPartitions]
     */
    Map<String, BrokerData> getCurrentBrokers();
    void close();
}
