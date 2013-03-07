package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.KafkaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicBrokersReader {

    public static final Logger LOG = LoggerFactory.getLogger(DynamicBrokersReader.class);

    CuratorFramework _curator;
    String _zkPath;
    String _topic;

    public DynamicBrokersReader(Map conf, String zkStr, String zkPath, String topic) {
        try {
            _zkPath = zkPath;
            _topic = topic;
            _curator = CuratorFrameworkFactory.newClient(
                    zkStr,
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                    15000,
                    new RetryNTimes(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
            _curator.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, BrokerData> getBrokerInfo() {
        Map<String, BrokerData> ret = new HashMap<String, BrokerData>();
        try {

            // get the list of brokers in Zookeeper, only to find one we can reach using Kafka's APIs
            //
            String brokerInfoPath = _zkPath + "/ids";
            List<String> children = _curator.getChildren().forPath(brokerInfoPath);
            for (String c : children) {
                try {
                    byte[] brokers = _curator.getData().forPath(brokerInfoPath + "/" + c);
                    JSONParser parser = new JSONParser();
                    JSONObject json = (JSONObject) parser.parse(new String(brokers, "UTF-8"));

                    String host = (String) json.get("host");
                    long lPort = (Long) json.get("port");
                    int port = (int) lPort;     // UGGH JSON wants the port to be a long, but everything else expects an int
                    ret = KafkaUtils.findMasters(host, port, _topic);

                    if (ret != null) break;

                } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                    LOG.error("Zookeeper error finding Brokers to build Partition data.", e);
                } catch (Exception e) {
                    // Lots can go wrong when trying to talk to Kafka to get the partition data. Don't error
                    // yet, see if another broker has what we want.
                    LOG.info("Exception caught building list of brokers and partitions. May not be fatal: ", e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (ret == null) {
            LOG.error("Unable to locate brokers and partitions for the topic: " + _topic);
        }
        return ret;
    }

    public void close() {
        _curator.close();
    }


}
