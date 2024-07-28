package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansClusteringHadoop {

    public static class MapperKMeans extends Mapper<Object, Text, FloatWritable, FloatWritable> {
        private final List<Float> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Example centroids initialization for simplification
            centroids.add(15.0f);
            centroids.add(25.0f);
            centroids.add(35.0f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Float dataPoint = Float.parseFloat(value.toString());
            Float nearestCentroid = null;
            float nearestDistance = Float.MAX_VALUE;

            for (Float centroid : centroids) {
                float distance = Math.abs(centroid - dataPoint);
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    nearestCentroid = centroid;
                }
            }

            context.write(new FloatWritable(dataPoint), new FloatWritable(nearestCentroid));
        }
    }

    public static class ReducerKMeans extends Reducer<FloatWritable, FloatWritable, FloatWritable, FloatWritable> {
        @Override
        public void reduce(FloatWritable dataPoint, Iterable<FloatWritable> centroids, Context context) throws IOException, InterruptedException {
            for (FloatWritable centroid : centroids) {
                context.write(dataPoint, centroid);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Clustering Hadoop");
        job.setJarByClass(KMeansClusteringHadoop.class);
        job.setMapperClass(MapperKMeans.class);
        job.setReducerClass(ReducerKMeans.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
