package storm.kafka.trident;

import java.net.ConnectException;
import java.util.*;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.ReducedMetric;
import com.google.common.collect.ImmutableMap;

import backtype.storm.utils.Utils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerData;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.trident.operation.TridentCollector;

public class KafkaUtils {
    // Constants for the names used in the Metadata being passed around
    //
    public static final String META_TOPOLOGY = "topology";
    public static final String META_TOPOLOGY_ID = "id";
    public static final String META_TOPOLOGY_NAME = "name";
    public static final String META_START_OFFSET = "offset";
    public static final String META_END_OFFSET = "nextOffset";
    public static final String META_INSTANCE_ID = "instanceId";
    public static final String META_PARTITION = "partition";
    public static final String META_BROKER = "broker";
    public static final String META_BROKER_HOST = "host";
    public static final String META_BROKER_PORT = "port";
    public static final String META_TOPIC = "topic";

    public static final long NOT_MASTER_BROKER = -1000;


    public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

    public static IBrokerReader makeBrokerReader(Map stormConf, TridentKafkaConfig conf) {
        if (conf.hosts instanceof StaticHosts) {
            return new StaticBrokerReader((StaticHosts) conf.hosts);
        } else {
            return new ZkBrokerReader(stormConf, conf.topic, (ZkHosts) conf.hosts);
        }
    }

    public static List<GlobalPartitionId> getOrderedPartitions(Map<String, BrokerData> partitions) {
        List<GlobalPartitionId> ret = new ArrayList<GlobalPartitionId>();
        TreeMap<String, BrokerData> allPartitions = new TreeMap<String, BrokerData>(partitions);
        for (String host : allPartitions.keySet()) {
            BrokerData info = allPartitions.get(host);
            long port = info.getPort();
            for (int partition : info.getPartitions()) {
                HostPort hp = new HostPort(host, (int) port);
                ret.add(new GlobalPartitionId(hp, partition));
            }
        }
        return ret;
    }

    public static ByteBufferMessageSet fill(SimpleConsumer consumer, String clientName, String topic,
                                            int partition, long offset, int fetchSize) {
        FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                .addFetch(topic, partition, offset, fetchSize)
                .build();
        return consumer.fetch(req).messageSet(topic, partition);

    }

    public static Map emitPartitionBatchNew(TridentKafkaConfig config, SimpleConsumer consumer, GlobalPartitionId partition,
                                            TridentCollector collector, Map lastMeta, String topologyInstanceId, String topologyName,
                                            ReducedMetric meanMetric, CombinedMetric maxMetric) {
        long newStartOffset = 0;
        long newStopOffset = 0;
        long lastStartOffset = 0;
        long lastStopOffset = 0;
        if (lastMeta != null) {
            String lastInstanceId = null;
            Map lastTopoMeta = (Map) lastMeta.get(META_TOPOLOGY);
            if (lastTopoMeta != null) {
                lastInstanceId = (String) lastTopoMeta.get(KafkaUtils.META_TOPOLOGY_ID);
            }
            if (config.forceFromStart && !topologyInstanceId.equals(lastInstanceId)) {
                newStartOffset = getLastOffset(consumer, config.topic, partition.partition, config.startOffsetTime,
                        config.clientName);
                lastStartOffset = lastStopOffset = newStartOffset;
            } else {
                lastStartOffset = (Long) lastMeta.get(KafkaUtils.META_START_OFFSET);
                lastStopOffset = (Long) lastMeta.get(META_END_OFFSET);
                // Since the last offset is the last one read, we need to start from the next offset. Note that this
                // gets ugly if we don't read anything since the +1 offset will be valid at some point in the future
                //
                newStartOffset = lastStopOffset + 1;
            }

        } else {
            // We don't have any data, so figure out where to start from
            long startTime = kafka.api.OffsetRequest.LatestTime();
            if (config.forceFromStart) startTime = config.startOffsetTime;
            newStartOffset = getLastOffset(consumer, config.topic, partition.partition, startTime, config.clientName);
            lastStartOffset = lastStopOffset = newStartOffset;
        }

        long numRead = 0;
        if (newStartOffset != NOT_MASTER_BROKER) {
            // no idea where to start the read since the Master isn't who we thought it was
            //
            ByteBufferMessageSet msgs;
            try {
                long start = System.nanoTime();
                msgs = fill(consumer, config.clientName, config.topic, partition.partition, newStartOffset,
                        config.fetchSizeBytes);
                long end = System.nanoTime();
                long millis = (end - start) / 1000000;
                meanMetric.update(millis);
                maxMetric.update(millis);
            } catch (Exception e) {
                if (e instanceof ConnectException) {
                    throw new FailedFetchException(e);
                } else {
                    throw new RuntimeException(e);
                }
            }

            for (MessageAndOffset msg : msgs) {
                emit(config, collector, msg.message());
                newStopOffset = msg.offset();
                numRead++;
            }
        }

        if (numRead == 0) {
            // nothing was found, so return the offsets to where they started
            //
            newStartOffset = lastStartOffset;
            newStopOffset = lastStopOffset;
        }

        Map newMeta = new HashMap();
        newMeta.put(KafkaUtils.META_START_OFFSET, newStartOffset);
        newMeta.put(META_END_OFFSET, newStopOffset);
        newMeta.put(META_INSTANCE_ID, topologyInstanceId);
        newMeta.put(KafkaUtils.META_PARTITION, partition.partition);
        newMeta.put(META_BROKER, ImmutableMap.of(KafkaUtils.META_BROKER_HOST, partition.host.host, KafkaUtils.META_BROKER_PORT, partition.host.port));
        newMeta.put(KafkaUtils.META_TOPIC, config.topic);
        newMeta.put(KafkaUtils.META_TOPOLOGY, ImmutableMap.of(KafkaUtils.META_TOPOLOGY_NAME, topologyName, KafkaUtils.META_TOPOLOGY_ID, topologyInstanceId));
        return newMeta;
    }

    public static void emit(TridentKafkaConfig config, TridentCollector collector, Message msg) {
        Iterable<List<Object>> values =
                config.scheme.deserialize(Utils.toByteArray(msg.payload()));
        if (values != null) {
            for (List<Object> value : values)
                collector.emit(value);
        }
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
                new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        long[] offsets = response.offsets(topic, partition);
        if (offsets.length == 0) {
            // This broker isn't correct for this client, so return an error code so the callers don't try to process
            // the dynamic broker discovery will figure out where this partition's new Master resides
            //
            return KafkaUtils.NOT_MASTER_BROKER;
        }
        return offsets[0];
    }

    public static void debugMeta(String a_caller, Map meta) {
        // write out what is in the meta data
        //
        LOG.info("Debugging meta data. Called from: " + a_caller);
        LOG.info(META_TOPIC + " = " + meta.get(META_TOPIC));
        LOG.info(META_PARTITION + " = " + meta.get(META_PARTITION));
        LOG.info(META_START_OFFSET + " = " + meta.get(META_START_OFFSET));
        LOG.info(META_END_OFFSET + " = " + meta.get(META_END_OFFSET));

        LOG.info(META_INSTANCE_ID + " = " + meta.get(META_INSTANCE_ID));

        Map top = (Map) meta.get(META_TOPOLOGY);
        LOG.info(META_TOPOLOGY_ID + " = " + top.get(META_TOPOLOGY_ID));
        LOG.info(META_TOPOLOGY_NAME + " = " + top.get(META_TOPOLOGY_NAME));

        Map broker = (Map) meta.get(META_BROKER);
        LOG.info(META_BROKER_HOST + " = " + broker.get(META_BROKER_HOST));
        LOG.info(META_BROKER_PORT + " = " + broker.get(META_BROKER_PORT));


    }

    public static Map<String, BrokerData> findMasters(String host, int port, String topic) {

        Map<String, BrokerData> ret = new HashMap<String, BrokerData>();
        kafka.javaapi.consumer.SimpleConsumer consumer = new SimpleConsumer(host, port,
                100000,
                64 * 1024, "test");

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

        List<kafka.javaapi.TopicMetadata> metaData = resp.topicsMetadata();

        for (kafka.javaapi.TopicMetadata item : metaData) {

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {

                String leader = part.leader().host();
                BrokerData broker;
                if (ret.containsKey(leader)) {
                    broker = ret.get(leader);
                } else {
                    broker = new BrokerData(part.leader().port());
                    ret.put(leader, broker);
                }
                broker.addPartition(part.partitionId());
            }
        }

        if (ret.size() == 0) return null;
        return ret;
    }


    public static class KafkaOffsetMetric implements IMetric {
        Map<GlobalPartitionId, Long> _partitionToOffset = new HashMap<GlobalPartitionId, Long>();
        Set<GlobalPartitionId> _partitions;
        String _topic;
        String _clientName;
        DynamicPartitionConnections _connections;

        public KafkaOffsetMetric(String topic, DynamicPartitionConnections connections, String clientName) {
            _topic = topic;
            _connections = connections;
            _clientName = clientName;
        }

        public void setLatestEmittedOffset(GlobalPartitionId partition, long offset) {
            _partitionToOffset.put(partition, offset);
        }

        @Override
        public Object getValueAndReset() {
            try {
                long totalSpoutLag = 0;
                long totalLatestTimeOffset = 0;
                long totalLatestEmittedOffset = 0;
                HashMap ret = new HashMap();
                if (_partitions != null && _partitions.size() == _partitionToOffset.size()) {
                    for (Map.Entry<GlobalPartitionId, Long> e : _partitionToOffset.entrySet()) {
                        GlobalPartitionId partition = e.getKey();
                        SimpleConsumer consumer = _connections.getConnection(partition);
                        if (consumer == null) {
                            LOG.warn("partitionToOffset contains partition not found in _connections. Stale partition data?");
                            return null;
                        }
                        long latestTimeOffset = getLastOffset(consumer, _topic, partition.partition, OffsetRequest.LatestTime(), _clientName);
                        if (latestTimeOffset == KafkaUtils.NOT_MASTER_BROKER) {
                            LOG.warn("No data found in Kafka Partition " + partition.getId());
                            return null;
                        }
                        long latestEmittedOffset = (Long) e.getValue();
                        long spoutLag = latestTimeOffset - latestEmittedOffset;
                        ret.put(partition.getId() + "/" + "spoutLag", spoutLag);
                        ret.put(partition.getId() + "/" + "latestTime", latestTimeOffset);
                        ret.put(partition.getId() + "/" + "latestEmittedOffset", latestEmittedOffset);
                        totalSpoutLag += spoutLag;
                        totalLatestTimeOffset += latestTimeOffset;
                        totalLatestEmittedOffset += latestEmittedOffset;
                    }
                    ret.put("totalSpoutLag", totalSpoutLag);
                    ret.put("totalLatestTime", totalLatestTimeOffset);
                    ret.put("totalLatestEmittedOffset", totalLatestEmittedOffset);
                    return ret;
                } else {
                    LOG.info("Metrics Tick: Not enough data to calculate spout lag.");
                }
            } catch (Throwable t) {
                LOG.warn("Metrics Tick: Exception when computing kafkaOffset metric.", t);
            }
            return null;
        }

        public void refreshPartitions(Set<GlobalPartitionId> partitions) {
            _partitions = partitions;
            Iterator<GlobalPartitionId> it = _partitionToOffset.keySet().iterator();
            while (it.hasNext()) {
                if (!partitions.contains(it.next())) it.remove();
            }
        }
    }


}
