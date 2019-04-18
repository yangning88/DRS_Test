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


public class sCounter extends BaseBasicBolt {
	 private Map<String, Integer> _counts = new HashMap<String, Integer>();
	 long count=0;
	 long complex=0;
	 public void execute(Tuple tuple, BasicOutputCollector collector) {
		 
	     String word = (String) tuple.getValueByField("word");
	    // System.out.println("--------the "+(count++)+" st-------- is ：   "+word);
	     int a=word.toLowerCase().toString().length();
	      complex +=a*a;
	      
	     double res=Math.sqrt(complex);
	     /*if(_counts.containsKey(word)){
	         count = _counts.get(word);
	     } else {
	         count = 0;
	     }*/
	     count ++;
	   //  _counts.put(word, count);
	     collector.emit(new Values(count));
	 }

	 public void declareOutputFields(OutputFieldsDeclarer declarer) {
	     declarer.declare(new Fields( "count"));
	 }
	}
