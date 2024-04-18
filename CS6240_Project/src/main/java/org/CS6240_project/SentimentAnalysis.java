package org.CS6240_project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;
import org.tartarus.snowball.ext.PorterStemmer;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.net.URI;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysis {
    public static class SentimentWritable implements Writable {
        private int score;
        private int positiveCount;
        private int negativeCount;

        public SentimentWritable() {
            this.score = 0;
            this.positiveCount = 0;
            this.negativeCount = 0;
        }

        public SentimentWritable(int score, int positiveCount, int negativeCount) {
            this.score = score;
            this.positiveCount = positiveCount;
            this.negativeCount = negativeCount;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(score);
            out.writeInt(positiveCount);
            out.writeInt(negativeCount);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            score = in.readInt();
            positiveCount = in.readInt();
            negativeCount = in.readInt();
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public int getPositiveCount() {
            return positiveCount;
        }

        public void setPositiveCount(int positiveCount) {
            this.positiveCount = positiveCount;
        }

        public int getNegativeCount() {
            return negativeCount;
        }

        public void setNegativeCount(int negativeCount) {
            this.negativeCount = negativeCount;
        }

        @Override
        public String toString() {
            return score + "\t" + positiveCount + "\t" + negativeCount;
        }
    }

    public static class SentimentMapper extends Mapper<LongWritable, Text, Text, SentimentWritable> {
        private Set<String> positiveWords = new HashSet<>();
        private Set<String> negativeWords = new HashSet<>();
        private PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            readWordsFromHDFS(positiveWords, new Path("/input/positive.txt"), fs);
            readWordsFromHDFS(negativeWords, new Path("/input/negative.txt"), fs);
}

        private void readWordsFromHDFS(Set<String> wordSet, Path path, FileSystem fs) throws IOException {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    wordSet.add(line.trim().toLowerCase());
                }
            }
        }

        private Set<String> sampleWords(Set<String> words) {
            return words.stream().limit(10).collect(Collectors.toSet());
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 3) {
                String userId = parts[0];
                String word = parts[1].toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
                int frequency = Integer.parseInt(parts[2]);

                if (word.isEmpty()) return;

                stemmer.setCurrent(word);
                stemmer.stem();
                String stemmedWord = stemmer.getCurrent();

                int sentimentScore = 0;
                int positiveCount = 0;
                int negativeCount = 0;
                if (positiveWords.contains(stemmedWord)) {
                    sentimentScore = frequency;  // Frequency affects the score
                    positiveCount = 1;           // Count each word only once regardless of frequency
                } else if (negativeWords.contains(stemmedWord)) {
                    sentimentScore = -frequency; // Negative words reduce the score, affected by frequency
                    negativeCount = 1;           // Count each word only once
                }

                context.write(new Text(userId), new SentimentWritable(sentimentScore, positiveCount, negativeCount));
            }
        }
    }

    public static class SentimentReducer extends Reducer<Text, SentimentWritable, Text, SentimentWritable> {
        public void reduce(Text key, Iterable<SentimentWritable> values, Context context) throws IOException, InterruptedException {
            int totalScore = 0;
            int totalPositive = 0;
            int totalNegative = 0;
            for (SentimentWritable val : values) {
                totalScore += val.getScore();
                totalPositive += val.getPositiveCount();
                totalNegative += val.getNegativeCount();
            }
            context.write(key, new SentimentWritable(totalScore, totalPositive, totalNegative));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SentimentAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SentimentWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SentimentWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

