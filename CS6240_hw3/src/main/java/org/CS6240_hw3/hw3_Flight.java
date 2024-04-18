package org.CS6240_hw3;

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class hw3_Flight {
    // Filtering the flight data by using the map function
    public static class FlightFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text flightDate = new Text();
        private Text flightDetail = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                CSVParser csvParser = new CSVParser();
                String[] fields = csvParser.parseLine(value.toString());

                // Process data rows here
                String Year = fields[0];
                String Month = fields[2];
                String flightDateStr = fields[5];
                String origin = fields[11];
                String dest = fields[17];
                String depTime = fields[24];
                String arrTime = fields[35];
                String arrDelayMinutes = fields[37];
                String cancelled = fields[41];
                String diverted = fields[43];

                if (origin.equals("ORD") && dest.equals("JFK")) {
                    return;
                }
                if (((origin.equals("ORD") || dest.equals("JFK")) && cancelled.equals("0.00") && diverted.equals("0.00") && isBetweenJune2007AndMay2008(Year, Month))) {
                    flightDate.set(flightDateStr);
                    flightDetail.set(flightDateStr + "," + origin + "," + dest + "," + depTime + "," + arrTime + "," + arrDelayMinutes);
                    context.write(new Text(Month), flightDetail);
                }
            } catch (Exception e) {
                context.write(new Text("error"), new Text(e.getMessage()));;
            }
        }

        private boolean isBetweenJune2007AndMay2008(String year, String month) {
            int numericYear = Integer.parseInt(year);
            int numericMonth = Integer.parseInt(month);
            return (numericYear == 2007 && numericMonth >= 6) || (numericYear == 2008 && numericMonth <= 5);
        }
    }

   // Partitioning the flight data by month
    public static class F1F2Partitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            System.out.println("Generating partition for key: " + key.toString());
            try {
                return Integer.parseInt(key.toString());
            } catch (Exception e) {
                throw new IllegalArgumentException("key:" + key.toString() + " value:" + value.toString());
            }
        }
    }

    // Use "self-join" to find out the matched F1 F2 pairs and sum the delay time.
    public static class F1F2PairReducer extends Reducer<Text, Text, Text, Text> {

        private List<String[]> leg1 = new ArrayList<>();
        private List<String[]> leg2 = new ArrayList<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                for (Text flight : values) {
                    String[] f = flight.toString().split(",");
                    if ("ORD".equals(f[1])) {
                        leg1.add(f);
                    } else {
                        leg2.add(f);
                    }
                }
            } catch (Exception e) {
                context.write(new Text("error"), new Text(e.getMessage()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                for (String[] f1 : leg1) {
                    for (String[] f2 : leg2) {
                        String flightDate1 = f1[0];
                        String dest1 = f1[2];
                        String arrTime1 = f1[4];
                        float arrDelayMin1 = Float.parseFloat(f1[5]);
                        String flightDate2 = f2[0];
                        String origin2 = f2[1];
                        String depTime2 = f2[3];
                        float arrDelayMin2 = Float.parseFloat(f2[5]);

                        if (flightDate1.equals(flightDate2) && dest1.equals(origin2) && compareTime(arrTime1, depTime2) < 0) {
                            int totalDelay = (int) (arrDelayMin1 + arrDelayMin2);
                            context.write(new Text("(F1F2)"), new Text("" + totalDelay));
                        }
                    }
                }
            } catch (Exception e) {
                context.write (new Text("error"), new Text(e.getMessage()));
            }
        }

        private int compareTime(String time1, String time2) {
            int hour1 = Integer.parseInt(time1.substring(0, 2));
            int hour2 = Integer.parseInt(time2.substring(0, 2));
            int minute1 = Integer.parseInt(time1.substring(2));
            int minute2 = Integer.parseInt(time2.substring(2));

            if (hour1 < hour2) {
                return -1;
            } else if (hour1 > hour2) {
                return 1;
            } else {
                if (minute1 < minute2) {
                    return -1;
                } else if (minute1 > minute2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }

    // Create a new mapper class fort job 2 to iterate the output from job 1 reducer.
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String delay = fields[1];
            context.write(new Text(fields[0]), new Text(delay));
        }
    }

    // Use reducer to calculate the average delay for each month for job 2
    public static class CountAndAverageDelayReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();
        private int totalDelay = 0;
        private int countPairs = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                totalDelay += Integer.parseInt(val.toString());
                countPairs++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (countPairs > 0) {
                double averageDelay = (double) totalDelay / countPairs;
                result.set("Count: " + countPairs + ", Average Delay: " + averageDelay);
            } else {
                result.set("No data");
            }
            context.write(new Text("final result"), result);
        }
    }

    // Define the job1 and job2 in this main class.
    public static void main(String[] args) throws Exception {
            // Job 1
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "Flight Filtering");

            job1.setJarByClass(hw3_Flight.class);

            job1.setMapperClass(FlightFilterMapper.class);
            job1.setReducerClass(F1F2PairReducer.class);
            // Set 13 reduce tasks
            job1.setNumReduceTasks(13);
            job1.setPartitionerClass(F1F2Partitioner.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path(args[0]));
            Path intermediateOutputPath = new Path("intermediate_output");

            FileOutputFormat.setOutputPath(job1, intermediateOutputPath);
            job1.waitForCompletion(true);

            //Job 2
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Flight delay calculation");

            job2.setJarByClass(hw3_Flight.class);
            job2.setMapperClass(MapperClass.class);
            job2.setReducerClass(CountAndAverageDelayReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, intermediateOutputPath); // Use the intermediateOutputPath
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
