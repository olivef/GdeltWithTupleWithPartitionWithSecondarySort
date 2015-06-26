package com.oliveirf.finalproject;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.*;

import org.apache.hadoop.io.*;

public class GdeltWithTuple {

    public static class GdeltMapper extends Mapper<LongWritable, Text, OutTuple, NullWritable> {
        private Text countryCode = new Text();
        private Text date = new Text();
        private static Hashtable<String, Integer> myhash = new Hashtable<String, Integer>();
	private NullWritable nullValue = NullWritable.get();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valor = value.toString().split("\t");
            countryCode.set(valor[44]);
            date.set(valor[1]);
          //  System.err.println("--------------countrycode"+countryCode);
	    OutTuple myoutTuple = new OutTuple();
	    myoutTuple.setCountryCode(countryCode.toString());
	    myoutTuple.setDate(date.toString());
	    myoutTuple.setNumOfItems(1);
	    if (countryCode != null ){
	    	context.write(myoutTuple,nullValue);
	    }

	}
    }

    public static class CountryCodePartitioner extends Partitioner<OutTuple, NullWritable>{
	    @Override
		    public int getPartition(OutTuple myoutTuple, NullWritable nullWritable, int numReduceTasks) {
			    return myoutTuple.getCountryCode().hashCode() % numReduceTasks;
		    }
    }    


 public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
	    super(OutTuple.class, true);
    }
    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
		    OutTuple ip1 = (OutTuple) w1;
		    OutTuple ip2 = (OutTuple) w2;
		    return ip1.getCountryCode().compareTo(ip2.getCountryCode()); //reverse
	    }
 }


 public static class CompositeKeyComparator extends WritableComparator {
	 protected CompositeKeyComparator() {
		 super(OutTuple.class, true);
	 }
	 @SuppressWarnings("rawtypes")
		 @Override
		 public int compare(WritableComparable w1, WritableComparable w2) {

			 OutTuple key1 = (OutTuple) w1;
			 OutTuple key2 = (OutTuple) w2;

			 // (first check on udid)
			 int compare = key1.getCountryCode().compareTo(key2.getCountryCode());
			 System.err.println("key1 country code  "+key1.getCountryCode()+"key2 countrycode    "+key2.getCountryCode()+"date key 1   "+key1.getDate()+"date key2  "+key2.getDate());
			 if (compare == 0) {
				 // only if we are in the same input group should we try and sort by value (datetime)
				 return -1*key1.getDate().compareTo(key2.getDate());
			 }

			 return compare;
		 }
 }

 public static class CustomReducer extends Reducer<OutTuple,NullWritable, Text, Text> {
	 private static Hashtable<String, Integer> myhash = new Hashtable<String, Integer>();    

	 MultipleOutputs<Text, Text> mos;
	 @Override
		 public void setup(Context context) {
			 mos = new MultipleOutputs(context);
		 }

	 @Override
		 public void reduce(OutTuple key,Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {
				Integer occurencies = myhash.get(key.getCountryCode()+key.getDate());
				//System.err.println("date="+val.getDate());
				if (occurencies == null) {
					myhash.put(key.getCountryCode()+key.getDate(), key.getNumOfItems());
					//System.err.println("e null");
				} else {
					myhash.put(key.getCountryCode()+key.getDate(), occurencies + key.getNumOfItems());
				}

				for (String hkey : myhash.keySet()) {
					OutTuple myoutTuple = new OutTuple();
					myoutTuple.setCountryCode(hkey.substring(0,2));
					myoutTuple.setDate(hkey.substring(2,hkey.length()));
					//System.err.println("Key: "+key+"valor   "+myhash.get(hkey));
					myoutTuple.setNumOfItems(myhash.get(hkey));
					if (myoutTuple.getCountryCode().equals("IN") || myoutTuple.getCountryCode().equals("US") || myoutTuple.getCountryCode().equals("GY") || myoutTuple.getCountryCode().equals("GZ")  ){
						mos.write(myoutTuple.getCountryCode(),new Text(myoutTuple.getCountryCode()),new Text(myoutTuple.getDate()+"\t"+myoutTuple.getNumOfItems()));
					}
				}
			}

	 @Override
		 protected void cleanup(Context context) throws IOException, InterruptedException {
			 mos.close();
		 }


 }

 public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
	 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	 Path input1 = new Path(otherArgs[0]);
	 Path output1 = new Path(otherArgs[1]);

	 Job job = Job.getInstance(conf);
	 job.setJarByClass(GdeltWithTuple.class);
	 job.setJobName("FinalProject");

	 job.setInputFormatClass(TextInputFormat.class);
	 job.setOutputFormatClass(TextOutputFormat.class);

	 job.setMapOutputKeyClass(OutTuple.class);
	 job.setMapOutputValueClass(NullWritable.class);
	 job.setPartitionerClass(CountryCodePartitioner.class);
	 job.setMapperClass(GdeltMapper.class);
	 FileInputFormat.addInputPath(job, input1);
	 //job.setCombinerClass(CalculateSUMReducer.class);
	 job.setReducerClass(CustomReducer.class);


	 job.setGroupingComparatorClass(KeyComparator.class);
	 job.setSortComparatorClass(CompositeKeyComparator.class);


	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);
	 TextOutputFormat.setOutputPath(job, output1);
	 MultipleOutputs.addNamedOutput(job, "IN", TextOutputFormat.class, Text.class, Text.class);
	 MultipleOutputs.addNamedOutput(job, "US", TextOutputFormat.class, Text.class, Text.class);
	 MultipleOutputs.addNamedOutput(job, "GY", TextOutputFormat.class, Text.class, Text.class);
	 MultipleOutputs.addNamedOutput(job, "GZ", TextOutputFormat.class, Text.class, Text.class);
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }

 public static class OutTuple implements WritableComparable<OutTuple> {
	 private String date;
	 private Integer numofitems;
	 private String countrycode;

	 public OutTuple(){
	 }

	 public OutTuple(String cc, String date){
		 this.date=date;
		 this.countrycode=cc;
	 }


	 public String getCountryCode() {
		 return countrycode;
	 }

	 public void setCountryCode(String s) {
		 this.countrycode = s;
	 }

	 public String getDate() {
		 return date;
	 }

	 public void setDate(String s) {
		 this.date = s;
	 }

	 public Integer getNumOfItems() {
		 return numofitems;
	 }

	 public void setNumOfItems(Integer t) {
		 this.numofitems = t;
	 }

	 @Override
		 public String toString() {
			 return (new StringBuilder()).append(countrycode).append(',').append(date).toString();
		 }

	 @Override
		 public int compareTo(OutTuple tp) {
			 int cmp = countrycode.compareTo(tp.countrycode);
			 if (cmp == 0) {
				 cmp = date.compareTo(tp.date);
			 }
			 return cmp;
		 }

	 @Override
		 public void readFields(DataInput in) throws IOException {
			 countrycode=in.readUTF(); 
			 date = in.readUTF();
			 numofitems = in.readInt();
		 }

	 @Override
		 public void write(DataOutput out) throws IOException {
			 out.writeUTF(countrycode);    
			 out.writeUTF(date);
			 out.writeInt(numofitems);
		 }




 }

}

