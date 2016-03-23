package com.rabin.spark.twitter;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat<String,Integer> {
	
	@Override
	public String generateActualKey(String key, Integer value) {
	    return key;
	}
	
	@Override
	public String generateFileNameForKeyValue(String key, Integer value, String name) {
	    return name;
	  }

}
