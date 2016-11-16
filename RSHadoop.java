
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class RSHadoop {

	public static void main(String[] args) throws Exception {
		  
		System.out.println("================ Step 1 ================");
	    System.out.println("Start processing the input data, prepare for the Euclidean Matrix");
	    System.out.println("conf1 && job1");
	    System.out.println("========================================");
	    
	    Configuration conf1 = new Configuration();
	    String[] IOArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
	    Job job1 = Job.getInstance(conf1, "Initialaze Recommendation System data");
	    job1.setJarByClass(RSHadoop.class);
	    job1.setMapperClass(InitMapper.class);
	    job1.setReducerClass(InitReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job1, new Path(IOArgs[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(IOArgs[0] + "/../Initialaze data"));
	    job1.waitForCompletion(true);
	    
	    System.out.println("================ Step 1 ================");
	    System.out.println("the inialaze part completed");
	    System.out.println("========================================");
	    
	    System.out.println("================ Step 2 ================");
	    System.out.println("Start establish the Euclidean Matrix");
	    System.out.println("conf2 && job2");
	    System.out.println("========================================");
	    
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "Establish the Euclidean Matrix");
	    job2.setJarByClass(RSHadoop.class);
	    job2.setMapperClass(EuclideanMapper.class);
	    job2.setReducerClass(EuclideanReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(IOArgs[0] + "/../Initialaze data"));
	    FileOutputFormat.setOutputPath(job2, new Path(IOArgs[0] + "/../Euclidean Matrix"));
	    job2.waitForCompletion(true);
	    
	    System.out.println("================ Step 2 ================");
	    System.out.println("Euclidean Matrix completed");
	    System.out.println("========================================");
	    
	    System.out.println("================ Step 3 ================");
	    System.out.println("Start establish user similarity Matrix");
	    System.out.println("conf3 && job3");
	    System.out.println("========================================");
	    
	    Configuration conf3 = new Configuration();
	    Job job3 = Job.getInstance(conf3, "Establish User Similarity Matrix");
	    job3.setJarByClass(RSHadoop.class);
	    job3.setMapperClass(SimilarityMapper.class);
	    job3.setReducerClass(SimilarityReducer.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job3, new Path(IOArgs[0] + "/../Euclidean Matrix"));
	    FileOutputFormat.setOutputPath(job3, new Path(IOArgs[0] + "/../User Similarity Matrix"));
	    job3.waitForCompletion(true);
	    
	    System.out.println("================ Step 3 ================");
	    System.out.println("User Similarity Matrix completed");
	    System.out.println("========================================");
	    
	    System.out.println("================ Step 4 ================");
	    System.out.println("Start establish recommendation places matrix");
	    System.out.println("conf4 && job4");
	    System.out.println("========================================");
	    
	    Configuration conf4 = new Configuration();
	    Job job4 = Job.getInstance(conf4, "Establish recommendation Matrix");
	    job4.setJarByClass(RSHadoop.class);
	    job4.setMapperClass(RecommendationMapper.class);
	    job4.setReducerClass(RecommendationReducer.class);
	    job4.setOutputKeyClass(Text.class);
	    job4.setOutputValueClass(Text.class);

	    FileInputFormat.setInputPaths(job4, new Path(IOArgs[0]),new Path(IOArgs[0]+"/../User Similarity Matrix"));
//	    System.out.println("add input paths");
	    
	    FileOutputFormat.setOutputPath(job4, new Path(IOArgs[0] + "/../Recomendation Matrix"));
//	    System.out.println("add output path");
	    job4.waitForCompletion(true);
	    
	    System.out.println("================ Step 4 ================");
	    System.out.println("Recommendation Matrix completed");
	    System.out.println("========================================");
	    
	    System.out.println("================ Step 5 ================");
	    System.out.println("final recommendation list");
	    System.out.println("conf5 && job5");
	    System.out.println("========================================");
	    
	    Configuration conf5 = new Configuration();
	    Job job5 = Job.getInstance(conf5, "Final recommendation list");
	    job5.setJarByClass(RSHadoop.class);
	    job5.setMapperClass(FinalMapper.class);
	    job5.setReducerClass(FinalReducer.class);
	    job5.setOutputKeyClass(Text.class);
	    job5.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job5, new Path(IOArgs[0] + "/../Recomendation Matrix"));
	    FileOutputFormat.setOutputPath(job5, new Path(IOArgs[1]));
	    job5.waitForCompletion(true);
	    
	    System.out.println("================ Step 5 ================");
	    System.out.println("Final recommendation list completed");
	    System.out.println("========================================");
	    
	    
	    
/*
	    Integer node_num = (int)job.getCounters().findCounter(InitMapper.Counter.NODE_NUM).getValue();
	    Double avgoutlink = (double)job.getCounters().findCounter(InitMapper.Counter.ONEWAY_LINK_NUM).getValue()
	    		/ (double)job.getCounters().findCounter(InitMapper.Counter.NODE_NUM).getValue();
	    Integer maxoutlink = (int) job.getCounters().findCounter(InitMapper.Counter.MAX_OUTLINK).getValue();
	    Integer minoutlink = (int) job.getCounters().findCounter(InitMapper.Counter.MIN_OUTLINK).getValue();
	    Integer oneway_link_num = (int)job.getCounters().findCounter(InitMapper.Counter.ONEWAY_LINK_NUM).getValue();
	    
	    long startTime = System.currentTimeMillis();
	    Integer iter_num=0;
	    int count = 0;
	    
	    
	    
	    System.out.println("start iteration");
	    //start iteration
	    
	    ArrayList<String> time = new ArrayList<>();
	    
	    
	    for(count=0;;){
	    	Configuration iteconf = new Configuration();
	    	

	        Job ite = Job.getInstance(conf, "Initialaze PageRank by Kayla");
	        ite.setJarByClass(PageRank.class);
	        ite.setMapperClass(PageRankMapper.class);
	        ite.setReducerClass(PageRankReducer.class);
	        
	        ite.setOutputKeyClass(Text.class);
	        ite.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(ite, new Path(IOArgs[0]+ "/../Ite_"+ count));
	        FileOutputFormat.setOutputPath(ite, new Path(IOArgs[0] + "/../Ite_"+(++count)));
	        ite.waitForCompletion(true);
	        
	        
	        long Converge = ite.getCounters().findCounter(PageRankReducer.Counter.CONV_CAL).getValue();
//	        System.out.println(Converge);
	        if(Converge < 1) {
	        	System.out.println("");
	        	System.out.println("------------------------------------------------------------");
	        	System.out.println("");
	        	System.out.println("This is the "+count+ " iteration time");
	        	System.out.println("PageRank has converged!");
	        	System.out.println("");
	        	System.out.println("------------------------------------------------------------");
	        	System.out.println("");
	        	break;
	        }
	        if(count > 50) {
	        	System.err.println("failed to converge");
		        System.exit(2);
	        }
	        System.out.println("interation times:" + count);
	        System.out.println("current scaled delta:" + Converge);
	        Long timespent = new Long(System.currentTimeMillis()-startTime);
	        time.add(Long.toString(timespent));
	        System.out.println("Total time consumes:");
	        System.out.println(Long.toString(timespent) + "ms");
	    }
	    
	    
	    System.out.println("");
	    System.out.println("------------------------------------------------------------");
	    System.out.println("");
	    System.out.println("node_Num: "+node_num);
	    System.out.println("avgoutlint_Num: "+avgoutlink);
	    System.out.println("maxoutlink: "+maxoutlink);
	    System.out.println("minoutlink: "+minoutlink);
	    System.out.println("oneway_link_num: "+oneway_link_num);
	    System.out.println("startTime: "+startTime);
	    System.out.println("");
	    System.out.println("------------------------------------------------------------");
	    System.out.println("");
	    System.out.println("==========================================");
	    System.out.println("cost time: "+time);
	    System.out.println("==========================================");
	    
	    //output result
	    Configuration rankingConf = new Configuration();
		Job rankingJob = new Job(rankingConf, "ranking job hadoop-0.20");
		rankingJob.setJarByClass(PageRank.class);
		rankingJob.setJarByClass(PageRank.class);
		rankingJob.setMapperClass(RankMapper.class);
		rankingJob.setNumReduceTasks(1);
		rankingJob.setOutputKeyClass(Text.class);
		rankingJob.setOutputValueClass(NullWritable.class);
		rankingJob.setSortComparatorClass(DescendingComp.class);
		FileInputFormat.addInputPath(rankingJob, new Path(IOArgs[0]+ "/../Ite_"+ count));
		FileOutputFormat.setOutputPath(rankingJob, new Path(IOArgs[1]));
		rankingJob.waitForCompletion(true);
	    System.out.println("Completed");
	    
*/	    
	    
	  }
	
}
