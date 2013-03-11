package storm.kafka.trident;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableMap;

import java.util.*;

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerData;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.KafkaCommunicationsException;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;


public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<Map<String, BrokerData>, GlobalPartitionId, Map> {
    public static final Logger LOG = LoggerFactory.getLogger(OpaqueTridentKafkaSpout.class);

    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();

    public OpaqueTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }

    @Override
    public IOpaquePartitionedTridentSpout.Emitter<Map<String, BrokerData>, GlobalPartitionId, Map> getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }

    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext tc) {
        return new Coordinator(conf);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    class Coordinator implements IOpaquePartitionedTridentSpout.Coordinator<Map> {
        IBrokerReader reader;

        public Coordinator(Map conf) {
            reader = KafkaUtils.makeBrokerReader(conf, _config);
        }

        @Override
        public void close() {
            _config.coordinator.close();
        }

        @Override
        public boolean isReady(long txid) {
            return _config.coordinator.isReady(txid);
        }

        @Override
        public Map getPartitionsForBatch() {
            boolean forceReload = false;
            return reader.getCurrentBrokers(forceReload);
        }
    }

    class Emitter implements IOpaquePartitionedTridentSpout.Emitter<Map<String, BrokerData>, GlobalPartitionId, Map> {
        DynamicPartitionConnections _connections;
        String _topologyName;
        KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;
        ReducedMetric _kafkaMeanFetchLatencyMetric;
        CombinedMetric _kafkaMaxFetchLatencyMetric;
        int retryCount = 3;     // TODO - move to a property

        IBrokerReader _brokerInterface = null;
        Map _configuration;

        public Emitter(Map conf, TopologyContext context) {
            _configuration = conf;
            _connections = new DynamicPartitionConnections(_config);
            _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_config.topic, _connections, _config.clientName);
            context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
            _kafkaMeanFetchLatencyMetric = context.registerMetric("kafkaFetchAvg", new MeanReducer(), 60);
            _kafkaMaxFetchLatencyMetric = context.registerMetric("kafkaFetchMax", new MaxMetric(), 60);
        }

        private void setupBrokerReader(Map conf) {
            if (_brokerInterface == null) {
                _brokerInterface = KafkaUtils.makeBrokerReader(conf, _config);
                boolean forceReload = true;
                _connections.setBrokerDetails(_brokerInterface.getCurrentBrokers(forceReload));
            }
        }

        @Override
        public Map emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map lastMeta) {
            int retries = 0;
            RuntimeException lastException = null;
            Map ret = null;
            while (retries < retryCount) {
                try {
                    setupBrokerReader(_configuration);
                    SimpleConsumer consumer = _connections.register(partition);
                    ret = KafkaUtils.emitPartitionBatchNew(_config, consumer, partition, collector, lastMeta, _topologyInstanceId, _topologyName, _kafkaMeanFetchLatencyMetric, _kafkaMaxFetchLatencyMetric);
                    _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long) ret.get(KafkaUtils.META_START_OFFSET));
                    break;
                } catch (FailedFetchException e) {
                    LOG.warn("Failed to fetch from partition " + partition);
                    if (lastMeta == null) {
                        return null;
                    } else {
                        ret = new HashMap();
                        ret.put(KafkaUtils.META_START_OFFSET, lastMeta.get(KafkaUtils.META_NEXT_OFFSET_TO_FETCH));
                        ret.put(KafkaUtils.META_NEXT_OFFSET_TO_FETCH, lastMeta.get(KafkaUtils.META_NEXT_OFFSET_TO_FETCH));
                        ret.put(KafkaUtils.META_PARTITION, partition.partition);
                        ret.put(KafkaUtils.META_BROKER, ImmutableMap.of(KafkaUtils.META_BROKER_HOST, partition.host.host, KafkaUtils.META_BROKER_PORT, partition.host.port));
                        ret.put(KafkaUtils.META_TOPIC, _config.topic);
                        ret.put(KafkaUtils.META_TOPOLOGY, ImmutableMap.of(KafkaUtils.META_TOPOLOGY_NAME, _topologyName, KafkaUtils.META_TOPOLOGY_ID, _topologyInstanceId));

                        return ret;
                    }
                } catch (KafkaCommunicationsException e) {
                    // Something happened talking to Kafka, reset the connections for this partition and try again
                    //
                    LOG.warn("Emit Replay: Exception received communicating with Kafka. Code: " + e.getReason() + " Retry: " + retries);
                    _connections.forceUnregister(partition);
                    boolean forceReload = true;
                    _connections.setBrokerDetails(_brokerInterface.getCurrentBrokers(forceReload));
                    retries++;
                    lastException = e;
                    KafkaUtils.debugMeta("Emit", lastMeta);
                } catch (RuntimeException re) {
                    LOG.warn("Emit Replay: Runtime Exception received communicating with Kafka. Retry: " + retries, re);
                    _connections.forceUnregister(partition);
                    boolean forceReload = true;
                    _connections.setBrokerDetails(_brokerInterface.getCurrentBrokers(forceReload));
                    retries++;
                    lastException = re;
                    KafkaUtils.debugMeta("Emit Replay", lastMeta);
                }
            }
            if (lastException != null) {
                LOG.error("Emit Replay: Giving up on Kafka!", lastException);
                KafkaUtils.debugMeta("Emit", lastMeta);
                throw lastException;
            }
            return ret;
        }

        @Override
        public void close() {
            _connections.clear();
        }

        @Override
        public List<GlobalPartitionId> getOrderedPartitions(Map<String, BrokerData> partitions) {
            return KafkaUtils.getOrderedPartitions(partitions);
        }

        @Override
        public void refreshPartitions(List<GlobalPartitionId> list) {
            _connections.clear();
            _kafkaOffsetMetric.refreshPartitions(new HashSet<GlobalPartitionId>(list));
        }
    }
}
