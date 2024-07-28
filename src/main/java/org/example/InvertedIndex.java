package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndex {

    // Mapper class for creating the inverted index
    public static class IndexMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text docId = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Get the name of the input file
            String filename = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
            docId.set(filename);

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, docId);
            }
        }
    }

    // Reducer class for creating the inverted index
    public static class IndexReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder indexBuilder = new StringBuilder();
            for (Text val : values) {
                if (indexBuilder.length() > 0) indexBuilder.append(", ");
                indexBuilder.append(val.toString());
            }
            result.set(indexBuilder.toString());
            context.write(key, result);
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
