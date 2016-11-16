
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

public class InitReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		
		context.write(new Text("place: "), key);
		Map<String, String> map = new HashMap<String,String>();
		for(Text value: values){
			String[] ValueList = value.toString().split(" ");
			if(ValueList.length >= 2){
				map.put(ValueList[0], ValueList[1]);
			}
		}
		
		Iterator<String> iter1 = map.keySet().iterator();
		while(iter1.hasNext()){
			String k1 = iter1.next();  
            String v1 = map.get(k1);  
            Iterator<String> iter2 = map.keySet().iterator();  
            while (iter2.hasNext())  
            {  
                String k2 = iter2.next();  
                String v2 = map.get(k2);  
                context.write(new Text(k1 + "," + k2), new Text(v1 + "," + v2));  
                
                System.out.println("---------InitReducer: key & value -------------");
				System.out.println(k1 + "," + k2);
				System.out.println(v1 + "," + v2);
            }  
		}
		
		/*
		 * input key & values : 1001 & <1,5.0  3,2.5 ...>
		 * 					  
		 * output key & value : 1,1 & 5.0,5.0
		 * 						1,3 & 5.0,2.5
		 * 						3,1 & 2.5 5.0
		 * 						3,3 & 2.5,2.5
		 */
	}

}
