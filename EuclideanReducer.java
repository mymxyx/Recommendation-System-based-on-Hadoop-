
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
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

public class EuclideanReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		/*
		 * UserSimilarity: similarity between two users
		 * count: number of sum
		 * sum: (xi-yi)*(xi-yi)
		 */
		
		double UserSimilarity = 0.0;
		int count = 0;
		double sum = 0.0;
		
		
		for(Text value: values){
			String[] ValueList = value.toString().split(",");
			if(ValueList.length>=2){
				sum += Math.pow(Double.parseDouble(ValueList[0])-Double.parseDouble(ValueList[1]),2);
				count++;
			}
		}
		
		// if two user id are the same, sum == 0 , similarity == 0
		if(sum != 0){
			UserSimilarity = count/(1 + Math.sqrt(sum));
		}
		
		context.write(key,new Text(Double.toString(UserSimilarity)));
		
		 System.out.println("---------EuclideanReducer: key & value -------------");
		 System.out.println(key);
		 System.out.println(UserSimilarity);
		
		
		/*
		 * similarity = n / ( 1 + sqrt(sum of pow(xi-yi)))
		 * 
		 * input key & value : 1,1 & <5.0,5.0  3.0,3.0  ...>
		 * 					   1,3 & <5.0,2.5  ...>
		 * 					   3,1 & <2.5 5.0  ...>
		 * 					   3,3 & <2.5,2.5  ...>
		 * 
		 * output key & value : 1,1	0.0
		 *					    1,2	0.6076559869667391
		 *					    1,3	0.2857142857142857
		 * 
		 */
		
		
		
	}

}
