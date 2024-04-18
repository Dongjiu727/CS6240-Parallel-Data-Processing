package org.CS6240_project;

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.regex.Pattern;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class UserTermFrequency {

    public static class TermFrequencyMapper extends Mapper<LongWritable, Text, UserFrequencyPair, Text> {
        // private static final Pattern ENGLISH_PATTERN = Pattern.compile("^[a-zA-Z]+$");

        private Text termText = new Text();
        public static boolean isEnglishString(String input) {
            // Regular expression that matches English alphabet letters, digits, and basic punctuation
            String englishPattern = "^\"?[a-zA-Z]+\"?$";
            return input.matches(englishPattern);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length >= 3) {
                String userId = fields[0].trim();
                String term = fields[1].trim();  //.replaceAll("^\"|\"$", "");
                String frequencyStr = fields[2].trim();

                // System.out.println("Processing term: " + term + " and frequency: " + frequencyStr);

                if (isEnglishString(term) && frequencyStr.matches("\\d+")) {
                    int frequency = Integer.parseInt(frequencyStr);
                    UserFrequencyPair userFrequencyPair = new UserFrequencyPair(userId, frequency);

                    termText.set(term); // Convert String to Text
                    context.write(userFrequencyPair, termText); // Correctly write UserFrequencyPair and Text
                }
            }
        }
    }

    public static class UserFrequencyPair implements WritableComparable<UserFrequencyPair> {
        private Text userId;
        private IntWritable frequency;

        public UserFrequencyPair() {
            this.userId = new Text();
            this.frequency = new IntWritable();
        }

        public UserFrequencyPair(String userId, int frequency) {
            this.userId = new Text(userId);
            this.frequency = new IntWritable(frequency);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            userId.write(out);
            frequency.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            userId.readFields(in);
            frequency.readFields(in);
        }

        @Override
        public int compareTo(UserFrequencyPair other) {
            int compareValue = this.userId.compareTo(other.userId);
            if (compareValue != 0) {
                return compareValue;
            }
            // Note: We sort frequencies in descending order here
            return -1 * this.frequency.compareTo(other.frequency);
        }

        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(UserFrequencyPair.class, true);
            }

            @Override
            public int compare(WritableComparable a, WritableComparable b) {
                return a.compareTo(b);
            }
        }

        @Override
        public int hashCode() {
            return this.userId.hashCode() * 163 + this.frequency.get();
        }

        public Text getUserId() {
            return userId;
        }

        public IntWritable getFrequency() {
            return frequency;
        }
    }




    public static class TermFrequencyReducer extends Reducer<UserFrequencyPair, Text, Text, IntWritable> {

        private Text lastUserId = new Text("not_set");
        private int termCount;

        public void reduce(UserFrequencyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (!lastUserId.equals(key.getUserId())) {
                lastUserId.set(key.getUserId());
                termCount = 0;
            }

            if (termCount < 10) {
                for (Text value : values) {
                    if (termCount < 10) {
                        context.write(new Text(key.getUserId() + "\t" + value.toString()), new IntWritable(key.getFrequency().get()));
                        termCount++;
                    }
                    if (termCount >= 10) break;
                }
            }
        }
    }


    public static class UserPartitioner extends Partitioner<UserFrequencyPair, Text> {
        @Override
        public int getPartition(UserFrequencyPair key, Text value, int numPartitions) {
            // Ensure that partitioning is based solely on the user ID
            return (key.getUserId().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static class CompositeKeyComparator extends WritableComparator {
        protected CompositeKeyComparator() {
            super(UserFrequencyPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            UserFrequencyPair k1 = (UserFrequencyPair) a;
            UserFrequencyPair k2 = (UserFrequencyPair) b;
            return k1.compareTo(k2);
        }
    }

    public static class CompositeKeyGroupingComparator extends WritableComparator {
        protected CompositeKeyGroupingComparator() {
            super(UserFrequencyPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            UserFrequencyPair k1 = (UserFrequencyPair) a;
            UserFrequencyPair k2 = (UserFrequencyPair) b;
            return k1.getUserId().compareTo(k2.getUserId());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Term Frequency Analysis");

        job.setJarByClass(UserTermFrequency.class);
        job.setMapperClass(TermFrequencyMapper.class);

        job.setPartitionerClass(UserPartitioner.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
        job.setReducerClass(TermFrequencyReducer.class);

        job.setNumReduceTasks(6);

        job.setMapOutputKeyClass(UserFrequencyPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileInputFormat.addInputPath(job, new Path("/Users/xiexiaoyang/hadoop/input/twitter_data.csv"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //FileOutputFormat.setOutputPath(job, new Path("/Users/xiexiaoyang/hadoop/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
