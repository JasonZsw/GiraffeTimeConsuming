package com.onebank.giraffe;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.onebank.common.GlobContans;

/**
 * 计算每条msg的耗时的Map程序
 * @author com.onebank.www
 * @time 2017-9-4 21:22
 */
public class SystemTimeConsumingMap extends Mapper<LongWritable, Text, Text, Text>{

	Text mapKey = null;
	Text mapValue = null;
	//String day = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//Configuration conf = new Configuration();
		//String date = conf.get("date");
		mapKey = new Text();
		mapValue = new Text();
		//day = context.getConfiguration().get("date");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		
		if (line.length() < GlobContans.LOG_MIN_LENGTH) return;
		
		JSONObject log = JSON.parseObject(line);
		
		if (log.get("msgId") == null || log.get("msgId").toString() == "" || log.get("recvSystemIp") == null || log.get("recvSystemIp").toString() == "") return;
		if (log.get("recvSystemId") == null || log.get("recvSystemId").toString() == "" || log.get("sendTimestamp") == null || log.get("sendTimestamp").toString() == "") return;
		if (log.get("recvTimestamp") == null || log.get("recvTimestamp").toString() == "" || log.get("SHLogDate") == null || log.get("SHLogDate").toString() == "") return;
		if (log.get("UTCLogDate") == null || log.get("UTCLogDate").toString() == "" || log.get("apiType") == null || log.get("apiType").toString() == "") return;
		if (log.get("apiVersion") == null || log.get("apiVersion").toString() == "" || log.get("bizSeqNo") == null || log.get("bizSeqNo").toString() == "") return;
		if (log.get("brokerStoreTimestamp") == null || log.get("brokerStoreTimestamp").toString() == "" || log.get("consumerSeqNo") == null || log.get("consumerSeqNo").toString() == "") return;
		if (log.get("currentTime") == null || log.get("currentTime").toString() == "" || log.get("host") == null || log.get("host").toString() == "" ) return;
		if (log.get("msgType") == null || log.get("msgType").toString() == "" || log.get("type") == null || log.get("type").toString() == "") return;
		if (log.get("logType") == null || log.get("logType").toString() == "" ) return;
		
		mapKey.set(log.get("msgId").toString());
		
		mapValue.set(line);
		
		context.write(mapKey, mapValue);
	}
	
}
