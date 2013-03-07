package storm.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.consumer.SimpleConsumer;


public class DynamicPartitionConnections {
    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet<Integer>();

        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }

    Map<HostPort, ConnectionInfo> _connections = new HashMap<HostPort, ConnectionInfo>();
    KafkaConfig _config;

    // This is a bit of a hack. Since this interface is storing the broker in the GlobalPartitionId, changes
    // to the leader for a specific partition can't be easily made in the event of a failure. So we keep
    // a mapping of the brokers and ignore where Trident/Storm thinks the partition is.
    //
    // TODO This needs to be fixed, but I can't find any documentation about where the Partition information is stored or
    // how to update it once we find a missing broker
    //

    Map<Integer, HostPort> _partitionToBroker = new HashMap<Integer, HostPort>();

    public DynamicPartitionConnections(KafkaConfig config) {
        _config = config;
    }

    public void setBrokerDetails(Map<String, BrokerData> a_brokers) {

        _partitionToBroker.clear();
        for (String broker: a_brokers.keySet()) {
            BrokerData data = a_brokers.get(broker);
            HostPort host = new HostPort(broker,data.getPort());
            for (int partition: data.getPartitions()) {
                _partitionToBroker.put(partition, host);
            }
        }
    }

    public  boolean ready() {
        if (_partitionToBroker.size() != 0) return true;
        return false;
    }

    public SimpleConsumer register(GlobalPartitionId id) {
        return register(id.partition);
    }

    private ConnectionInfo getConnectionInfo(int partition) {
        HostPort broker = _partitionToBroker.get(partition);

        if (!_connections.containsKey(broker)) {
            _connections.put(broker, new ConnectionInfo(new SimpleConsumer(broker.host, broker.port, _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientName)));
        }
        return _connections.get(broker);
    }

    private SimpleConsumer register(int partition) {
        ConnectionInfo info = getConnectionInfo(partition);
        info.partitions.add(partition);
        return info.consumer;
    }

    public SimpleConsumer getConnection(GlobalPartitionId id) {
        ConnectionInfo info = getConnectionInfo(id.partition);
        if (info != null) return info.consumer;
        return null;
    }

    private void unregister(int partition) {
        HostPort broker = _partitionToBroker.get(partition);
        ConnectionInfo info = _connections.get(broker);
        if (info != null) {
            info.partitions.remove(partition);
            if (info.partitions.isEmpty()) {
                info.consumer.close();
                _connections.remove(broker);
            }
        }
    }

    public void forceUnregister(GlobalPartitionId id) {
        HostPort broker = _partitionToBroker.get(id.partition);
        ConnectionInfo info = _connections.get(broker);
        if (info != null) {
            info.partitions.clear();
        }
        unregister(id.partition);
    }

    public void unregister(GlobalPartitionId id) {
        unregister(id.partition);
    }

    public void clear() {
        for (ConnectionInfo info : _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
