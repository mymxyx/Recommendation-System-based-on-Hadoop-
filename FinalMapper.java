
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FinalMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String regex = ",|\\s+";
			String[] tokens = value.toString().split(regex);  
			if(tokens.length>=3){
				String k = tokens[0];
				String v = tokens[1] + "," + tokens[2];
				
				
				System.out.println("---------FinalMapper: key & value -------------");
				System.out.println(k);
				System.out.println(v);
				
				context.write(new Text(k), new Text(v));
			}
			
			/*
			 * input value:  3	1002,1.8333333333333333
			 * 
			 * output key & value : 
			 */
			
			
		}

}
