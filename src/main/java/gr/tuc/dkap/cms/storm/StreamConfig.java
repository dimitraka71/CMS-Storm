package gr.tuc.dkap.cms.storm;

/**
 * Declarations for streams
 */
public class StreamConfig {

    // Stream to send data/tuples from CMSSpout to CMSBolt
    public static final String DATA_STREAM="dataStream";
    // Stream to indicate EOF of tuples
    public static final String TICK_STREAM="tickStream";
    // Stream to send topK estimations from CMSBolt to Aggregator bolt
    public static final String TOP_K_STREAM="topKStream";
    // Stream to send custom user ids from CMSSpout to CMSBolt
    public static final String CUSTOM_USER_IDS_STREAM ="customUserIdsStream";
    // Stream to send custom user ids count from CMSBolt spout to Aggregator
    public static final String CUSTOM_USER_IDS_COUNT_STREAM ="customUserIdsCountStream";
}
