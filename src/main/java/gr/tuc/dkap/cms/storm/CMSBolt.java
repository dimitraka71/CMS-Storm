package gr.tuc.dkap.cms.storm;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import gr.tuc.dkap.cms.storm.utils.CountMinSketch;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.UUID;

/**
 * A bolt that setups CountMinSketch and adds new tuples using CMS algorithm.
 * When a tick value arrives it sends the frequency of the tick value
 */
public class CMSBolt implements IRichBolt {
    private static final Logger LOG = Logger.getLogger(CMSBolt.class);

    private CountMinSketch cms;
    private OutputCollector collector;
    private int topK = 100;
    private Long processedTuples = 0L;
    private Long startTime;
    private Boolean eof = false;
    private String boltId = "test";

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamConfig.TOP_K_STREAM, new Fields("topK"));
        outputFieldsDeclarer.declareStream(StreamConfig.CUSTOM_USER_IDS_COUNT_STREAM, new Fields("userId", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // Setup count min sketch
        double errorLimit = Double.parseDouble(map.get("errorLimit").toString());
        double errorProbLimit = Double.parseDouble(map.get("errorProbabilityLimit").toString());
        topK = Integer.parseInt(map.get("topK").toString());
        cms = new CountMinSketch(errorLimit, errorProbLimit);
        boltId = UUID.randomUUID().toString();
        collector = outputCollector;
        startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {

        // If tick tuple, just log statistics for bolt
        if (isTickTuple(tuple)) {
            if (eof) return;
            double processedTuplesPerSecond = ((double) processedTuples / ((System.currentTimeMillis() - startTime))) * 1000;
            LOG.info(boltId + " -> Total processed tuples [" + processedTuples + "]");
            LOG.info(boltId + " -> Total topK tuples in queue [" + cms.getTopKValuesCount() + "]");
            LOG.info(boltId + " -> Processing rate: [" + (int) processedTuplesPerSecond + "]  tuples per second ");
            return;
        }

        // If message from tick stream, send top k results to TopKStream
        if (tuple.getSourceStreamId().equals(StreamConfig.TICK_STREAM)) {
            LOG.info("Received message from TickStream. Sending top K results...");
            Map<Object, Long> topKValues = cms.getTopK(topK);
            collector.emit(StreamConfig.TOP_K_STREAM, new Values(serializeTopKValuesAsOneMessage(topKValues)));
            eof = true;
        }

        // If a custom user id is received, estimate it's frequency and emit frequency to next stream
        if (tuple.getSourceStreamId().equals(StreamConfig.CUSTOM_USER_IDS_STREAM)) {
            String userId = tuple.getStringByField("userId");
            try {
                Long estimatedCount = cms.estimateCount(Long.parseLong(userId));
                collector.emit(StreamConfig.CUSTOM_USER_IDS_COUNT_STREAM, new Values(userId, estimatedCount));
            } catch (Exception e) {
                LOG.trace(e.getMessage());
            }
        }

        // If user id is received, update it's count using count min sketch
        if (tuple.getSourceStreamId().equals(StreamConfig.DATA_STREAM)) {
            processedTuples++;
            String userId = tuple.getStringByField("userId");
            try {
                cms.add(Long.parseLong(userId));
            } catch (Exception e) {
            }
        }

    }

    /**
     * Creates a json list with all the topK values, so they all are in one message
     *
     * @param topKValues the topK values map
     */
    private String serializeTopKValuesAsOneMessage(Map<Object, Long> topKValues) {
        return new Gson().toJson(topKValues);
    }

    /**
     * Check whether this tuple is a tick tuple
     *
     * @param tuple the tuple
     * @return true if it is a tick tuple else false
     */
    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void cleanup() {

    }

}