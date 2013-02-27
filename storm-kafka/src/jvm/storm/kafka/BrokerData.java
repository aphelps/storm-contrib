package storm.kafka;

import java.util.ArrayList;
import java.util.List;

public class BrokerData {
    private long port;
    private List<Integer> partitions;

    public BrokerData(long a_port) {
        port = a_port;
        partitions = new ArrayList<Integer>();
    }
    public void addPartition(int a_partition) {
        partitions.add(a_partition);
    }

    public long getPort() {
        return port;
    }
    public List<Integer> getPartitions() {
        return partitions;
    }
}
