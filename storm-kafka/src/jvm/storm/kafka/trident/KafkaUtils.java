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
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
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
        FetchResponse response = consumer.fetch(req);
        if (response.hasError()) {
            throw new KafkaCommunicationsException(response.errorCode(topic, partition));
        }
        return response.messageSet(topic, partition);
    }

    public static Map emitPartitionBatchNew(TridentKafkaConfig config, SimpleConsumer consumer, GlobalPartitionId partition,
                                            TridentCollector collector, Map lastMeta, String topologyInstanceId, String topologyName,
                                            ReducedMetric meanMetric, CombinedMetric maxMetric) {
        long newStartOffset;
        long newStopOffset = 0;

        if (lastMeta != null) {
            String lastInstanceId = null;
            Map lastTopoMeta = (Map) lastMeta.get(META_TOPOLOGY);
            if (lastTopoMeta != null) {
                lastInstanceId = (String) lastTopoMeta.get(KafkaUtils.META_TOPOLOGY_ID);
            }
            if (config.forceFromStart && !topologyInstanceId.equals(lastInstanceId)) {
                //  the Instance Id check is to make sure that we aren't starting from scratch if we've already
                // done that for this topology.
                newStartOffset = getLastOffset(consumer, config.topic, partition.partition, config.startOffsetTime,
                        config.clientName);
            } else {
                // Since the last offset is the last one read, we need to start from the next offset.
                //
                newStartOffset = (Long) lastMeta.get(META_END_OFFSET) + 1;
            }

        } else {
            // We don't have any data, so figure out where to start from
            long startTime = kafka.api.OffsetRequest.LatestTime();
            if (config.forceFromStart) startTime = config.startOffsetTime;
            newStartOffset = getLastOffset(consumer, config.topic, partition.partition, startTime, config.clientName);
        }

        long numRead = 0;

        ByteBufferMessageSet msgs;
        try {
            long start = System.nanoTime();
            msgs = fill(consumer, config.clientName, config.topic, partition.partition, newStartOffset,
                    config.fetchSizeBytes);
            long end = System.nanoTime();
            long millis = (end - start) / 1000000;
            meanMetric.update(millis);
            maxMetric.update(millis);
        } catch (KafkaCommunicationsException ke) {
            throw ke;
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

        if (numRead == 0) {
            // nothing was found, so return the offsets to where they started
            //
           return lastMeta;
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

        if (response.hasError()) {
            throw new KafkaCommunicationsException(response.errorCode(topic, partition));
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public static void debugMeta(String a_caller, Map meta) {
        // write out what is in the meta data
        //
        String msg = "Debugging meta data. Called from: " + a_caller + "\n\t[ " + META_TOPIC + " = " + meta.get(META_TOPIC) + "\n"
                + "\t" + META_PARTITION + " = " + meta.get(META_PARTITION) + "\n"
                + "\t" + META_START_OFFSET + " = " + meta.get(META_START_OFFSET) + "\n"
                + "\t" + META_END_OFFSET + " = " + meta.get(META_END_OFFSET) + "\n"

                + "\t" + META_INSTANCE_ID + " = " + meta.get(META_INSTANCE_ID) + "\n";

        Map top = (Map) meta.get(META_TOPOLOGY);
        msg += "\t" + META_TOPOLOGY_ID + " = " + top.get(META_TOPOLOGY_ID) + "\n"
                + "\t" + META_TOPOLOGY_NAME + " = " + top.get(META_TOPOLOGY_NAME) + "\n";

        Map broker = (Map) meta.get(META_BROKER);
        msg += "\t" + META_BROKER_HOST + " = " + broker.get(META_BROKER_HOST) + "\n"
                + "\t" + META_BROKER_PORT + " = " + broker.get(META_BROKER_PORT) + "\n";
        LOG.info(msg);


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
                        long latestEmittedOffset = e.getValue();
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
            } catch (KafkaCommunicationsException e) {
                LOG.warn("Communications Error communicating with Kafka. Kafka code: " + e.getReason());
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
