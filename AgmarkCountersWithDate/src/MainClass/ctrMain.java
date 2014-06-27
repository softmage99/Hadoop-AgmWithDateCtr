package MainClass;



import java.io.IOException;

import map.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import reduce.Reduce;



public class ctrMain {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		JobConf conf = new JobConf(ctrMain.class);
		 conf.setJobName("counter");
		 
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(IntWritable.class);
		 
		 conf.setMapperClass(Map.class);
		 conf.setCombinerClass(Reduce.class);
		 conf.setReducerClass(Reduce.class);
		 
		 conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(TextOutputFormat.class);
		 
		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 
		 JobClient.runJob(conf);
	}

}
