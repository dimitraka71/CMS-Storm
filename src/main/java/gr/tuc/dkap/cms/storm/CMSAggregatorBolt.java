package gr.tuc.dkap.cms.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.*;

/**
 * An aggregator bolt that merges top K results from each bolt and logs the global top K results
 */
public class CMSAggregatorBolt implements IRichBolt {
    private static final Logger LOG = Logger.getLogger(CMSAggregatorBolt.class);

    private int topK; // number of top K values to extract
    private int totalCMSBolts; // number of cmc bolts that process the stream
    private int totalCMSBoltsSentTopK; // number of cmc bolts that have finished processing stream
    protected Map<String, Long> allTopK = new HashMap<String, Long>();  // all top k values from all bolts

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        topK = Integer.parseInt(map.get("topK").toString());
        totalCMSBolts = Integer.parseInt(map.get("totalCMSBolts").toString());
    }

    @Override
    public void execute(Tuple tuple) {

        // If custom user id count is received just log it
        if (tuple.getSourceStreamId().equals(StreamConfig.CUSTOM_USER_IDS_COUNT_STREAM)) {
            LOG.info("User id [" + tuple.getStringByField("userId") + "] count [" + tuple.getLongByField("count") + "]");
        }

        // Log top K results when all have been received
        if (tuple.getSourceStreamId().equals(StreamConfig.TOP_K_STREAM)) {

            // Add received topK results to sorted map
            Map<String, Long> data = deserializeTopKValues(tuple.getStringByField("topK"));
            LOG.info("Received [" + data.size() + "] topK results from bolt");
            for (Map.Entry<String, Long> entry : data.entrySet()) {
                allTopK.put(entry.getKey(), entry.getValue());
            }

            // Check if all bolts have send their topK results and then extract global topK results
            totalCMSBoltsSentTopK++;
            if (totalCMSBoltsSentTopK == totalCMSBolts) {
                logTopKResults(allTopK, topK);
            }
        }

    }

    /**
     * Sorts given map by value and logs the topK values
     *
     * @param map       the map with the data
     * @param topKToLog the values to logs
     */
    private void logTopKResults(Map<String, Long> map, int topKToLog) {
        int index = 1;
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {

            @Override
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
            }
        });

        Map<Object, Object> globalTopKResults = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            globalTopKResults.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Object, Object> entry : globalTopKResults.entrySet()) {
            if (index <= topKToLog) {
                System.out.println(index + "," + entry.getKey() + "," + entry.getValue() + "");
            }
            index++;
        }
    }

    /**
     * Deserializes json string map to java.util.Map
     *
     * @param jsonData the topK as json map
     * @return the Map with topK results
     */
    private Map<String, Long> deserializeTopKValues(String jsonData) {
        Type type = new TypeToken<Map<String, Long>>() {
        }.getType();
        return new Gson().fromJson(jsonData, type);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    @Override
    public void cleanup() {
    }
}