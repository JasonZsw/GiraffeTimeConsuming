package com.onebank.giraffe;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.onebank.common.GlobContans;


/**
 * 计算每条msg的耗时的reduce程序
 * @author com.onebank.www
 * @time 2017-9-4 21:22
 */
public class SystemTimeConsumingReduce extends Reducer<Text, Text, Text, NullWritable>{

	Text mapKey = null;
	String date = null ;

	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mapKey = new Text();
		date = context.getConfiguration().get("date");
		//System.out.println(date);
	}
	
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Boolean sendflag = false ;
		Boolean recvflag = false ;
		JSONObject sendline = null ;
		JSONObject recvline = null ;
		
		for (Text text : value) {
			//根据msgtype来计算当前msg的耗时
			JSONObject line = JSON.parseObject(text.toString());
			if (line.get("msgType").toString().equals("0")){
				sendline = JSON.parseObject(text.toString());
				sendflag = true ; 
			} else if (line.get("msgType").toString().equals("1")) {
				recvline = JSON.parseObject(text.toString());
				recvflag = true ; 
			}
		}
		if (sendflag && recvflag){

			Long consumTime = Long.parseLong(recvline.get("recvTimestamp").toString()) - Long.parseLong(sendline.get("sendTimestamp").toString());
			
			StringBuilder builder = new StringBuilder();
			
			builder.append(sendline.get("msgId").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("logType").toString()).append(GlobContans.SPLIT_SIGN)
			.append(sendline.get("sendIp").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("recvSystemIp").toString()).append(GlobContans.SPLIT_SIGN)
			.append(sendline.get("recvSystemId").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("sendTimestamp").toString()).append(GlobContans.SPLIT_SIGN)
			.append(recvline.get("recvTimestamp").toString()).append(GlobContans.SPLIT_SIGN).append(consumTime.toString()).append(GlobContans.SPLIT_SIGN)
			.append(sendline.get("SHLogDate").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("UTCLogDate").toString()).append(GlobContans.SPLIT_SIGN)
			.append(sendline.get("apiType").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("apiVersion").toString()).append(GlobContans.SPLIT_SIGN)
			.append(sendline.get("bizSeqNo").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("brokerStoreTimestamp").toString()).append(GlobContans.SPLIT_SIGN)
			.append(sendline.get("consumerSeqNo").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("currentTime").toString()).append(GlobContans.SPLIT_SIGN)
			.append(sendline.get("host").toString()).append(GlobContans.SPLIT_SIGN).append(sendline.get("type").toString()).append(GlobContans.SPLIT_SIGN)
			.append(date);
			
			mapKey.set(builder.toString()); 
			context.write(mapKey, null);
		
		}
		
	}
}
