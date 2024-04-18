package org.CS6240_project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Classification {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text userId = new Text();
        private Text termAndFreq = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t");

            if (parts.length == 3) {
                userId.set(parts[0].replaceAll("^\"|\"$", ""));
                termAndFreq.set(parts[1].replaceAll("^\"|\"$", "") + "\t" + parts[2]);
                context.write(userId, termAndFreq);
            }
        }
    }

    public static class ClusterReducer extends Reducer<Text, Text, Text, IntWritable> {
        private int numClusters = 3; // Assuming 3 clusters for the example
        private List<Map<String, Double>> centroids; // Assuming each centroid is a vector in term space

        @Override
        protected void setup(Context context) {
            // This should be a well-thought-out strategy. In this example, we'll just make up some fake centroids.
            centroids = initializeCentroids(numClusters);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> termFrequencyVector = new HashMap<>();

            for (Text val : values) {
                String[] termFreq = val.toString().split("\\t");
                if (termFreq.length == 2) {
                    String term = termFreq[0];
                    double frequency = Double.parseDouble(termFreq[1]);
                    termFrequencyVector.put(term, frequency);
                }
            }

            int clusterId = assignToCluster(termFrequencyVector);
            context.write(key, new IntWritable(clusterId));
        }

        private List<Map<String, Double>> initializeCentroids(int numClusters) {
            List<Map<String, Double>> initialCentroids = new ArrayList<>();
            // ... (logic to initialize centroids based on your dataset) ...
            return initialCentroids;
        }

        private int assignToCluster(Map<String, Double> termFrequencyVector) {
            int closestCentroidIndex = -1;
            double smallestDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = calculateDistance(termFrequencyVector, centroids.get(i));
                if (distance < smallestDistance) {
                    smallestDistance = distance;
                    closestCentroidIndex = i;
                }
            }
            return closestCentroidIndex;
        }

        private double calculateDistance(Map<String, Double> vector1, Map<String, Double> vector2) {
            double sum = 0.0;
            // Simplest possible implementation: Euclidean distance
            for (String term : vector1.keySet()) {
                double freq1 = vector1.getOrDefault(term, 0.0);
                double freq2 = vector2.getOrDefault(term, 0.0);
                sum += (freq1 - freq2) * (freq1 - freq2);
            }
            return Math.sqrt(sum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Clustering");
        job.setJarByClass(Classification.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(ClusterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

