package gr.tuc.dkap.cms.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spout that reads data from URL and emits them to collector
 */
public class CMSSpout implements IRichSpout {
    private static final Logger LOG = Logger.getLogger(CMSSpout.class);
    private static final int LINES_TO_READ = 100000;
    private static final String PREDEFINED_IDS = "22629,1923,30527,29689,32790,1311,19512,4083,861,11354,16989,48281" +
            "13376,12881,16800,8049,4903,5291,39992,16668,26130,1084,20055,23909,28804,3827,20135,1329401,10326,36641";

    private BufferedReader inputFileReader;
    private SpoutOutputCollector collector;
    private Long delay;
    private Boolean eof;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamConfig.DATA_STREAM, new Fields("postId", "score", "userId"));
        outputFieldsDeclarer.declareStream(StreamConfig.TICK_STREAM, new Fields("eof"));
        outputFieldsDeclarer.declareStream(StreamConfig.CUSTOM_USER_IDS_STREAM, new Fields("userId"));
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        inputFileReader = startReader((String) map.get("dataUrl"));
        delay = (Long) map.get("delay");
        eof = false;
    }

    /**
     * Reads lines from file, extracts only needed values (user id etc) and emits to collector
     */
    @Override
    public void nextTuple() {
        if (eof) return;

        int index = 0;
        try {
            inputFileReader.readLine(); // skip xml metadata field
            inputFileReader.readLine(); // skip xml comments field


            while (true) {

                String line = inputFileReader.readLine();

                // if EOF -> send to TICK_STREAM finish message
                if (line.equalsIgnoreCase("comments") || index > LINES_TO_READ) {

                    // send tick that indicates eof
                    LOG.info("Spout has consumed all [" + index + "] rows from file. Emitting to tick message to data stream");
                    eof = true;
                    collector.emit(StreamConfig.TICK_STREAM, new Values(true));

                    // send custom user ids
                    for (String userId : PREDEFINED_IDS.split(",")) {
                        collector.emit(StreamConfig.CUSTOM_USER_IDS_STREAM, new Values(userId));

                    }
                    break;
                }

                index++;
                // Emit new tuple
                Thread.sleep(delay);
                collector.emit(StreamConfig.DATA_STREAM, createTuple(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Split line data and create tuple values with postId,score,userId
     *
     * @param line the xml data
     * @return the Values of the tuple
     */
    private Values createTuple(String line) {
        String[] data = line.split("\" ");
        String postId = data[1].substring(data[1].indexOf("\"") + 1, data[1].length());
        String score = data[2].substring(data[2].indexOf("\"") + 1, data[2].length());
        String userId = data[5].substring(data[5].indexOf("\"") + 1, data[5].length());
       return new Values(postId, score, userId);
    }

    /**
     * Setup a file reader to read from a URL
     *
     * @param sourceUrl the url to read from
     * @return the BufferedReader to read from
     */
    private BufferedReader startReader(String sourceUrl) {
        BufferedReader in = null;
        try {
            URL url = new URL(sourceUrl);
            in = new BufferedReader(new InputStreamReader(url.openStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return in;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }


}
