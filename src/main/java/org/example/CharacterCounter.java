package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class CharacterCounter {

    public static class CharCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text character = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                character.set(Character.toString(c));
                context.write(character, one);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable totalCount = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalCount.set(sum);
            context.write(key, totalCount);
        }
    }

    public static void main(String[] args) throws Exception {
        // Create a new configuration
        Configuration config = new Configuration();
        // Create a new job instance with the configuration and a descriptive name
        Job job = Job.getInstance(config, "Character Count");
        // Set the jar by class
        job.setJarByClass(CharacterCounter.class);
        // Set the mapper class
        job.setMapperClass(CharCountMapper.class);
        // Set the combiner class
        job.setCombinerClass(SumReducer.class);
        // Set the reducer class
        job.setReducerClass(SumReducer.class);
        // Set the output key class
        job.setOutputKeyClass(Text.class);
        // Set the output value class
        job.setOutputValueClass(IntWritable.class);
        // Add input path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Exit the job with the status of job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
