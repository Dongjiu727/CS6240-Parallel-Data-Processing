package org.CS6240_project;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;

public class DecisionTreePrediction {

    public static class PredictionMapper extends Mapper<LongWritable, Text, Text, Text> {
        private SerializeDecisionTreeModel.DecisionTreeModel model;

        @Override
        protected void setup(Context context) {
            // Initialize the model in a try-with-resources statement to handle exceptions
            try (InputStream is = getClass().getResourceAsStream("/decisionTreeModel.ser");
                 ObjectInputStream ois = new ObjectInputStream(is)) {
                model = (SerializeDecisionTreeModel.DecisionTreeModel) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                // Proper error handling: log this error or throw an unchecked exception
                System.err.println("Failed to load the decision tree model: " + e.getMessage());
                throw new RuntimeException("Failed to load decision tree model", e);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 4) {
                String userId = parts[0];
                int overallScore = Integer.parseInt(parts[1]);
                int positiveCount = Integer.parseInt(parts[2]);
                int negativeCount = Integer.parseInt(parts[3]);

                String prediction = model.makePrediction(overallScore, positiveCount, negativeCount);
                context.write(new Text(userId), new Text(prediction));
            }
        }
    }

    public static class PredictionReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DecisionTreePrediction <input path> <output path> <model path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Decision Tree Prediction");
        job.setJarByClass(DecisionTreePrediction.class);
        job.setMapperClass(PredictionMapper.class);
        job.setReducerClass(PredictionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
