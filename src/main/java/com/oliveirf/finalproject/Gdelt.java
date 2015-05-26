package com.oliveirf.finalproject;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;

public class Gdelt {

    public static class GdeltMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text countryCode = new Text();
        private double num_items;
        private Text date = new Text();
        
        private static Hashtable<String, Integer> myhash = new Hashtable<String, Integer>();


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valor = value.toString().split("\t");
            countryCode.set(valor[44]);
            date.set(valor[1]);
	    Integer occurencies = myhash.get(countryCode.toString()+date.toString());
            if (occurencies == null) {
            	myhash.put(countryCode.toString()+date.toString(), 1);
	    } else {
                myhash.put(countryCode.toString()+date.toString(), occurencies + 1);
              }
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String key : myhash.keySet()) {
			System.err.println("Key:"+key + myhash.get(key));
			context.write(new Text(key), new IntWritable(myhash.get(key)));
		} 
	}  
    }

    public static class CalculateSUMReducer extends Reducer<Text, IntWritable, Text, Text> {
	    @Override
		    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			   InterruptedException {
				   int sum = 0;
				   for (IntWritable val : values) {
					   sum += val.get();
				   }

				   context.write(new Text(key.toString().substring(0,2)),new Text(key.toString().substring(2,key.toString().length()) +"\t" + String.valueOf(sum)));
			   }
    }

    public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	    Path wikistats = new Path(otherArgs[0]);
	    Path join_result = new Path(otherArgs[1]);

	    Job job = Job.getInstance(conf);
	    job.setJarByClass(Gdelt.class);
	    job.setJobName("FinalProject");

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    job.setMapperClass(GdeltMapper.class);
	    FileInputFormat.addInputPath(job, wikistats);
	    //job.setCombinerClass(CalculateSUMReducer.class);
	    job.setReducerClass(CalculateSUMReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    TextOutputFormat.setOutputPath(job, join_result);

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

