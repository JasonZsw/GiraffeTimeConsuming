package com.onebank.giraffe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 驱动程序
 * @author com.onebank.www
 * @time 2017-9-4 21:22
 */
public class SystemTimeConsumingDriver {
	public static void main(String[] args) throws Exception {
		if(args.length != 4){
			System.err.println("input args counts error!!");
			System.err.println("please input jobName,inputpath,outputpath,date!!");
		    System.exit(0);
		}
		System.out.println(args[1]);
		Configuration conf = new Configuration();
		conf.set("date", args[3]);
		Job job = Job.getInstance(conf, args[0]);
		
		/*System.out.println(args[1]);
		System.out.println(conf.get("date"));*/
		job.setJarByClass(SystemTimeConsumingDriver.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(SystemTimeConsumingMap.class);
		job.setReducerClass(SystemTimeConsumingReduce.class);
		
		Path intputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, intputPath);	
		
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);	
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);
	}
}
