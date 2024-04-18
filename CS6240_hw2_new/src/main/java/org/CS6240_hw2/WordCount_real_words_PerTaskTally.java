package org.CS6240_hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


public class WordCount_real_words_PerTaskTally {
    /**
     * Mapper class for counting word occurrences per task.
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        // HashMap to store word counts
        private final HashMap<String, Integer>  WcMap = new HashMap<>();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // Check if the word starts with specified letters.
                String current_word = itr.nextToken();
                if (current_word.toLowerCase().startsWith("m") ||
                    current_word.toLowerCase().startsWith("n") ||
                    current_word.toLowerCase().startsWith("o") ||
                    current_word.toLowerCase().startsWith("p") ||
                    current_word.toLowerCase().startsWith("q")) {
                    // Increment the word count in the HashMap.
                    WcMap.put(current_word,WcMap.getOrDefault(current_word,0) + 1);
                    }
            }
        }

        /**
         * Emits (word, count) pairs from the accumulated word counts when the map task finishes.
         * @param context The context object for emitting output.
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String, Integer> entry: WcMap.entrySet()) {
                word.set(entry.getKey());
                context.write(word, new IntWritable(entry.getValue()));
            }
        }
    }

    public static class WordCountPartitioner extends Partitioner<Text, IntWritable> {
       public int getPartition(Text key, IntWritable value, int numPartitions) {
           // Logging statement to track the number of partitions being generated
           System.out.println("Generating partition for key: " + key.toString());

           // If the key length is 0, assign to partition 0.
           if (key.getLength() == 0) {
               return 0;
           }
           String real_word = key.toString().toLowerCase();
           if(real_word.charAt(0) == 'm') {
               return 0;
           }
           if(real_word.charAt(0) == 'n') {
               return 1 % numPartitions;
           }
           if(real_word.charAt(0) == 'o') {
               return 2 % numPartitions;
           }
           if(real_word.charAt(0) == 'p') {
               return 3 % numPartitions;
           }
           else{
               return 4 % numPartitions;
           }
       }
    }
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable results = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            results.set(sum);
            context.write(key, results);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount_real_words_PerTaskTally.class);
        job.setMapperClass(TokenizerMapper.class);
        // Disable the combiner
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(5);
        job.setPartitionerClass(WordCountPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}