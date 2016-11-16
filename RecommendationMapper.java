
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

public class RecommendationMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			/*
			 * this Mapper function input two kind of data set:
			 * 1) the original data set
			 * 2) data produced by UserSimilarity Matrix reducer
			 * 
			 * the places id need to be created in sequential numbers, which would help add to key more easily
			 * 
			 * since we have tow input data sets
			 * the number of output data sets (key-value pairs for input reducer) is also two
			 * 
			 * value start with "a:" is from the original data
			 * value start with "b:" is from UserSimilarity Matrix reducer data
			 */
			System.out.println("++++++++++++++++++++++++++++++++++++");
			System.out.println("this is RecommendationMapper");
			
			int PlaceIndex = 7;
			String line = value.toString();
			if(!line.contains(":")){
				
				StringTokenizer token = new StringTokenizer(value.toString());
				if(token.hasMoreTokens()){
					String v = "a"+","+token.nextToken();
					String k = token.nextToken();
					v = v + "," + token.nextToken();
					
					System.out.println("---------RecommendationMapper: key & value (original data)-------------");
					System.out.println(k);
					System.out.println(v);
					
					context.write(new Text(k), new Text(v));
				}
			}
			else{
				String regex = ",|:|\\s+";
				String[] tokens = value.toString().split(regex);
/*				
				System.out.println("============this is for test===============");
				System.out.println("this line is:" + value);
				System.out.println("tokens[0]:"+tokens[0]);
				System.out.println("tokens[1]:"+tokens[1]);
				System.out.println("tokens[2]:"+tokens[2]);
				System.out.println("tokens[3]:"+tokens[3]);
				System.out.println("tokens[4]:"+tokens[4]);
				System.out.println("tokens[5]:"+tokens[5]);
*/
				
				for(int i = 1;i<= PlaceIndex;i++){
					Text k = new Text(Integer.toString(1000+i));
					Text v = new Text("b"+","+tokens[1]+","+tokens[2]+","+tokens[4]+","+tokens[6]);
					
					System.out.println("---------RecommendationMapper: key & value -(from last step)------------");
					System.out.println(k);
					System.out.println(v);
					
					context.write(new Text(k), new Text(v));
				}
			}
			
			
			
			
			/*
			 * input value:  1 1001 5.0 
			 * input value: :1 & 5,1.4164078649987382 4,1.3333333333333333 2,0.6076559869667391
			 * 
			 * output key & value: 1001 & a1,5.0 
			 * output key & value: 1001 & b1,5,4,2
			 */
		}

}
