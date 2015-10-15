package gr.tuc.dkap.cms.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Topology builder
 */
public class CMSTopology {

    private static final Double ERROR=0.001d;
    private static final Double ERROR_LIMIT=0.999d;
    private static final int PARALLELISM_LEVEL=4;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

        System.out.println("Usage: jobName parallelismLevel K window");



        // Setup configuration object
        Config config = new Config();
        config.put("parallelismLevel", 1);
        config.put("delay", 0);
        config.put("errorLimit", ERROR);
        config.put("errorProbabilityLimit", ERROR_LIMIT);
        config.put("topK", 100);
        config.put("totalCMSBolts", PARALLELISM_LEVEL);   // http://localhost/sampleComments.xml
        config.put("dataUrl", "http://localhost/Comments.xml"); // http://10.11.12.163/stream/Comments.xml
        config.setDebug(false);
        config.setNumWorkers(1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("cmsFileSpout", new CMSSpout(), 1);

        // CMSBolt receives raw data from DataStream, using fields grouping on 'userId'
        // CMSBolt receives custom user ids from CustomUserIdsStream, using fields grouping on 'userId'
        // CMSBolt receives ticks from TickStream
        CMSBolt cmcBolt = new CMSBolt();
        builder.setBolt("cmsBolt", cmcBolt, PARALLELISM_LEVEL)
                .fieldsGrouping("cmsFileSpout", StreamConfig.DATA_STREAM, new Fields("userId"))
                .allGrouping("cmsFileSpout", StreamConfig.TICK_STREAM)
                .fieldsGrouping("cmsFileSpout", StreamConfig.CUSTOM_USER_IDS_STREAM, new Fields("userId"));

        // Aggregator receives topK estimated values from CmsBolts
        // Aggregator receives custom user ids estimated values from CmsBolts
        builder.setBolt("cmsAggregator", new CMSAggregatorBolt(), 1)
                .shuffleGrouping("cmsBolt", StreamConfig.TOP_K_STREAM)
                .shuffleGrouping("cmsBolt", StreamConfig.CUSTOM_USER_IDS_COUNT_STREAM);

        // Run locally
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("CMS", config, builder.createTopology());
        Thread.sleep(200000);
        cluster.shutdown();


    }
}
