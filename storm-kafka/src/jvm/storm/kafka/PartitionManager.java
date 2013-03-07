package storm.kafka;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import com.google.common.collect.ImmutableMap;

import java.util.*;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.trident.KafkaUtils;

public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    static class KafkaMessageId {
        public GlobalPartitionId partition;
        public long offset;

        public KafkaMessageId(GlobalPartitionId partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    Long _emittedToOffset;
    SortedSet<Long> _pending = new TreeSet<Long>();
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    GlobalPartitionId _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;


    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, GlobalPartitionId id) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id);
        _state = state;
        _stormConf = stormConf;

        String jsonTopologyId = null;
        Long jsonOffset = null;
        try {
            Map<Object, Object> json = _state.readJSON(committedPath());
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get(KafkaUtils.META_TOPOLOGY)).get(KafkaUtils.META_TOPOLOGY_ID);
                jsonOffset = (Long) json.get(KafkaUtils.META_START_OFFSET);
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + committedPath(), e);
        }

        if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = KafkaUtils.getLastOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime, spoutConfig.clientName);
            //   _committedTo = _consumer.getOffsetsBefore(spoutConfig.topic, id.partition, spoutConfig.startOffsetTime, 1)[0];
            LOG.info("Using startOffsetTime to choose last commit offset.");
        } else if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = KafkaUtils.getLastOffset(_consumer, spoutConfig.topic, id.partition, kafka.api.OffsetRequest.LatestTime(), spoutConfig.clientName);
            //  _committedTo = _consumer.getOffsetsBefore(spoutConfig.topic, id.partition, -1, 1)[0];
            LOG.info("Setting last commit offset to HEAD.");
        } else {
            _committedTo = jsonOffset;
            LOG.info("Read last commit offset from zookeeper: " + _committedTo);
        }

        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;
    }


    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if (_waitingToEmit.isEmpty()) fill();
        while (true) {
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }
            Iterable<List<Object>> tups = _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
            if (tups != null) {
                for (List<Object> tup : tups)
                    collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
                break;
            } else {
                ack(toEmit.offset);
            }
        }
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }

    private void fill() {
        ByteBufferMessageSet msgs = KafkaUtils.fill(_consumer, _spoutConfig.clientName, _spoutConfig.clientName, _partition.partition, _emittedToOffset, _spoutConfig.fetchSizeBytes);
        int numMessages = 0;
//        int numMessages = msgs.underlying().size();
//        if(numMessages>0) {
//          LOG.info("Fetched " + numMessages + " messages from Kafka: " + _consumer.host() + ":" + _partition.partition);
//        }
        for (MessageAndOffset msg : msgs) {
            _pending.add(_emittedToOffset);
            _waitingToEmit.add(new MessageAndRealOffset(msg.message(), _emittedToOffset));
            _emittedToOffset = msg.offset();
            numMessages++;
        }
        if (numMessages > 0) {
            LOG.info("Added " + numMessages + " messages from Kafka: " + _consumer.host() + ":" + _partition.partition + " to internal buffers");
        }
    }

    public void ack(Long offset) {
        _pending.remove(offset);
    }

    public void fail(Long offset) {
        //TODO: should it use in-memory ack set to skip anything that's been acked but not committed???
        // things might get crazy with lots of timeouts
        if (_emittedToOffset > offset) {
            _emittedToOffset = offset;
            _pending.tailSet(offset).clear();
        }
    }

    public void commit() {
        LOG.info("Committing offset for " + _partition);
        long committedTo;
        if (_pending.isEmpty()) {
            committedTo = _emittedToOffset;
        } else {
            committedTo = _pending.first();
        }
        if (committedTo != _committedTo) {
            LOG.info("Writing committed offset to ZK: " + committedTo);

            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put(KafkaUtils.META_TOPOLOGY, ImmutableMap.of(KafkaUtils.META_TOPOLOGY_ID, _topologyInstanceId,
                            KafkaUtils.META_TOPOLOGY_NAME, _stormConf.get(Config.TOPOLOGY_NAME)))
                    .put(KafkaUtils.META_START_OFFSET, committedTo)
                    .put(KafkaUtils.META_PARTITION, _partition.partition)
                    .put(KafkaUtils.META_BROKER, ImmutableMap.of(KafkaUtils.META_BROKER_HOST, _partition.host.host,
                            KafkaUtils.META_BROKER_PORT, _partition.host.port))
                    .put(KafkaUtils.META_TOPIC, _spoutConfig.topic).build();
            _state.writeJSON(committedPath(), data);

            LOG.info("Wrote committed offset to ZK: " + committedTo);
            _committedTo = committedTo;
        }
        LOG.info("Committed offset " + committedTo + " for " + _partition);
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition;
    }

    public long queryPartitionOffsetLatestTime() {
        return KafkaUtils.getLastOffset(_consumer, _spoutConfig.topic, _partition.partition,
                kafka.api.OffsetRequest.LatestTime(), _spoutConfig.clientName);
    }

    public long lastCommittedOffset() {
        return _committedTo;
    }

    public long lastCompletedOffset() {
        if (_pending.isEmpty()) {
            return _emittedToOffset;
        } else {
            return _pending.first();
        }
    }

    public GlobalPartitionId getPartition() {
        return _partition;
    }

    public void close() {
        _connections.unregister(_partition);
    }
}
