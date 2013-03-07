package storm.kafka;

import java.util.ArrayList;
import java.util.List;

public class BrokerData {
    private int port;
    private List<Integer> partitions;

    public BrokerData(int a_port) {
        port = a_port;
        partitions = new ArrayList<Integer>();
    }
    public void addPartition(int a_partition) {
        partitions.add(a_partition);
    }

    public int getPort() {
        return port;
    }
    public List<Integer> getPartitions() {
        return partitions;
    }
}
