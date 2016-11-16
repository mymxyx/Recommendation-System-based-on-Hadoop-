
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;

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

public class RecommendationReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		
		
		/*
		 * value start with "a:" is from the original data
	     * value start with "b:" is from UserSimilarity Matrix reducer data
		 */
		
		System.out.println("++++++++++++++++++++++++++++++++++++");
		System.out.println("this is RecommendationReducer");
		
		 HashMap<String, String> hashmapA = new HashMap<String,String>();
		 HashMap<String, String> hashmapB = new HashMap<String,String>();
		 String regex = ",|:|\\s+";
		 for(Text value: values){
			 String[] ValueList = value.toString().split(",");  
			 
			 System.out.println("");
			 System.out.println("ValueList[0]: "+ValueList[0]);
			 System.out.println("ValueList[1]: "+ValueList[1]);
			 System.out.println("ValueList[2]: "+ValueList[2]);

			 System.out.println("");
			 
			 if(ValueList[0].equals("a")){
				 
				 
				 System.out.println("valueList[1]:"+ValueList[1]);
				 System.out.println("ValueList[2]:"+ValueList[2]);

				 hashmapA.put(ValueList[1], ValueList[2]);
			 }
			 if(ValueList[0].equals("b")){
				
				 System.out.println("valueList[1]:"+ValueList[1]);
				 System.out.println("ValueList[2]:"+ValueList[2]);
				 System.out.println("valueList[3]:"+ValueList[3]);
				 System.out.println("ValueList[4]:"+ValueList[4]);

				 
				 hashmapB.put(ValueList[1],ValueList[2]+","+ValueList[3]+","+ValueList[4]);
			 }
		 }
		 
		 /*
		  * to each place
		  * hashmapA :  
		  * key & value: 1 & 5.0    (user & rating)
		  * 
		  * hashmapB :
		  * key & value: 1 & 5,4,2  (user & three similar users)
		  */
		 
		 System.out.println("iterate hashmapA");
		 for(String keyA: hashmapA.keySet()){
			 System.out.println("key=" + keyA + " and value= "+hashmapA.get(keyA));
		 }
		 System.out.println(" ");
		 System.out.println("iterate hashmapB");
		 for(String keyB: hashmapB.keySet()){
			 System.out.println("key=" + keyB + " and value= "+hashmapB.get(keyB));
		 }
		 
		
		 Iterator<String> iteB = hashmapB.keySet().iterator();
		 while(iteB.hasNext()){
			 String user = iteB.next();                                           // one place; for one user
			 if(!hashmapA.containsKey(user)){                                  //if this user didn't go the that place, get into the loop
				 String[] simiUsers = hashmapB.get(user).split(regex);            // find the corresponding similar users
				 if(simiUsers.length >= 3){
					 double v1 = hashmapA.containsKey(simiUsers[0]) ? Double.parseDouble(hashmapA.get(simiUsers[0])) : 0;
					 double v2 = hashmapA.containsKey(simiUsers[1]) ? Double.parseDouble(hashmapA.get(simiUsers[1])) : 0;
					 double v3 = hashmapA.containsKey(simiUsers[2]) ? Double.parseDouble(hashmapA.get(simiUsers[2])) : 0;
					 double finalRating = (v1+v2+v3)/3;
					 
					 Text k = new Text(user);
					 Text v = new Text(key.toString() + "," + finalRating);
					 
					 System.out.println("---------RecommendationReducer: key & value ------------");
					 System.out.println(k);
					 System.out.println(v);
						
					 context.write(k,v);
					 
				 } 
			 }
		 }
		
		
		/*
		  * input key & value: 1001 & <a1,5.0  ...>
		  * input key & value: 1001 & <b1,5,4,2 ...>
		  * 
		  * output key & value: 3 &	1002,1.8333333333333333
		 */
		
		
	}

}
