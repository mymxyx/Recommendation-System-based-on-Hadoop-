
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

public class SimilarityMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			// split the line with "," or space
			String regex = ",|\\s+";
			String[] tokens = value.toString().split(regex);  
			            
			if (tokens.length >= 3)  
			{  
				Text k = new Text(tokens[0]);
				Text v = new Text(tokens[1] + "," + tokens[2]);
				context.write(k,v);
				
				System.out.println("---------SimilarityMapper: key & value -------------");
				System.out.println(k);
				System.out.println(v);
				
			}  
			
			
			/*
			 * input value: 1,1	0.0
			 *				1,2	0.6076559869667391
			 *				1,3	0.2857142857142857
			 *
			 * output key & value: 1 & 1,0.0
			 * 					   1 & 2,0.6076559869667391
			 * 					   1 & 3,0.2857142857142857
			 */
			
		}

}
