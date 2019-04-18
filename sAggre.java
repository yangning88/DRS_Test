package topology;

import static topology.Constant.FIELD_IMG_BYTES;

import java.util.HashMap;
import java.util.Map;

import org.bytedeco.javacpp.opencv_core.Mat;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class sAggre extends BaseBasicBolt {
	 private Map<String, Integer> _counts = new HashMap<String, Integer>();
	 int count=0;
	 long complex=0;
	 public void execute(Tuple tuple, BasicOutputCollector collector) {
		 
	     long word = (long) tuple.getValueByField("count");
	    // System.out.println("--------the-result------ is ：   "+word);

	 }

	 public void declareOutputFields(OutputFieldsDeclarer declarer) {
	     declarer.declare(new Fields("word", "count"));
	 }
	}
