package topology;

import static resa.util.ConfigUtil.getInt;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

public class sTopology implements Constant {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
           // System.exit(0);
        }
   //  Config conf = ConfigUtil.readConfig(args[0]);
       Config conf = ConfigUtil.readConfig("/home/storm/ton-storm-09-drs-vd.yaml");
//        TopologyBuilder builder = new WritableTopologyBuilder();
        TopologyBuilder builder = new ResaTopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 7000);
        String queue = (String) conf.get("redis.queue");

        builder.setSpout("sSpout", new sSpout(host, port, queue), getInt(conf, "vd.spout.parallelism", 1));

        builder.setBolt("sCounter", new sCounter(), getInt(conf, "vd.feat-ext.parallelism", 1))
                .shuffleGrouping("sSpout")
                .setNumTasks(getInt(conf, "vd.feat-ext.tasks", 1));
        builder.setBolt("sAggre", new sAggre(), getInt(conf, "vd.aggregator.parallelism", 1))
        .shuffleGrouping("sCounter")
        .setNumTasks(getInt(conf, "vd.aggregator.tasks", 1));
       /* builder.setBolt("matcher", new Matcher(), getInt(conf, "vd.matcher.parallelism", 1))
                .allGrouping("feat-ext", STREAM_FEATURE_DESC)
                .setNumTasks(getInt(conf, "vd.matcher.tasks", 1));
        builder.setBolt("aggregator", new Aggregater(), getInt(conf, "vd.aggregator.parallelism", 1))
                .fieldsGrouping("feat-ext", STREAM_FEATURE_COUNT, new Fiel9+ds(FIELD_FRAME_ID))
                .fieldsGrouping("matcher", STREAM_MATCH_IMAGES, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, "vd.aggregator.tasks", 1));
*/
        int numWorkers = ConfigUtil.getInt(conf, "vd-worker.count", 1);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "vd-MaxSpoutPending", 0));
        conf.setStatsSampleRate(1.0);

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        if (ConfigUtil.getBoolean(conf, "vd.metric.resa", false)) {
            resaConfig.addDrsSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

        StormSubmitter.submitTopology("resa-yn", resaConfig, builder.createTopology());

    }

}

