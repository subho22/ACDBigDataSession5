package mapreduce.assignment.task7;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;

public class Task7 {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AssignmentTask7");
		job.setJarByClass(Task7.class);

		job.setMapOutputKeyClass(TV.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(TV.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Task7Mapper.class);
		job.setReducerClass(Task7Reducer.class);
		
		job.setNumReduceTasks(4);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}
