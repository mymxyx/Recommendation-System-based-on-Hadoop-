
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

public class SimilarityReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		
		HashMap<Double, String> hashmap = new HashMap<Double, String>();
		
		for (Text value : values) {  
            String[] ValueList = value.toString().split(",");  
              
            if (ValueList.length >= 2)  
            {  
                hashmap.put(Double.parseDouble(ValueList[1]), ValueList[0]);  
            }  
        }  
          
        List<Double> list = new ArrayList<Double>();  
        Iterator<Double> iter = hashmap.keySet().iterator();  
        while (iter.hasNext()) {  
            Double similarity = iter.next();  
            list.add(similarity);  
        }  
          
        //sort the similarity in descending order
        Collections.sort(list,new Comparator<Double>() {  
  
            public int compare(Double data1, Double data2) {  
                return data2.compareTo(data1);  
            }  
        });  
          
 
        // choose the first three users which have similarity attractive places' preferences. 
        String v = "";  
        for (int i = 0; i < 3 && i < list.size(); i++)  
        {  
            v += hashmap.get(list.get(i)) + "," + Double.toString(list.get(i)) + " ";  
        }  
        context.write(new Text(":"+key), new Text(v));  
        
        System.out.println("---------SimilarityReducer: key & value -------------");
		System.out.println(":"+key);
		System.out.println(v);
	
		/*
		 * input key & value : 1 & <1,0.0  2,.6076559869667391  3,0.2857142857142857>
		 *				
		 * output key & value: :1 & 5,1.4164078649987382 4,1.3333333333333333 2,0.6076559869667391
		 */
		
	}

}
