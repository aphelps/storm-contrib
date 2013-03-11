package storm.kafka.trident;

import storm.kafka.KafkaConfig;


public class TridentKafkaConfig extends KafkaConfig {
    public TridentKafkaConfig(BrokerHosts hosts, String topic, String clientName) {
        super(hosts, topic, clientName);
    }

    public IBatchCoordinator coordinator = new DefaultCoordinator();
}
