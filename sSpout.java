package topology;

import static topology.Constant.STREAM_IMG_OUTPUT;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import resa.topology.RedisQueueSpout;


public class sSpout extends RedisQueueSpout {
	 private SpoutOutputCollector collector;
	 private long frameId;
	 private String idPrefix;
	 private long acked;
	 private static final String[] MSGS = new String[]{
	         "Storm", "HBase", "Integration", "example", "by ", "yang", "ning", "april",
	 };

	 private static final Random random = new Random();


	 public sSpout(String host, int port, String queue) {
		// TODO Auto-generated constructor stub
		 super(host, port, queue, true);
	}
	  @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	     declarer.declare(new Fields("id","word"));
	 }

	 public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		 super.open(conf, context, collector);
	        this.collector = collector;
	        frameId = 0;
	        acked = 0;
	        idPrefix = String.format("s-%02d-", context.getThisTaskIndex() + 1);
	 }


	 public void nextTuple() {
	     String word = MSGS[random.nextInt(8)];
	     String id = idPrefix + frameId++;
	        collector.emit( new Values(id, word), id);
	     //collector.emit(new Values(word));
	     System.out.println("-----------------------------aaaa");
	 }
	}
