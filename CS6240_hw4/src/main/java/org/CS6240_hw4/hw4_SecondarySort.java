package org.CS6240_hw4;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class hw4_SecondarySort {
    // Filtering the flight data by using the map function

    public static class MonthlyDelayMapper extends Mapper<LongWritable, Text, CarrierMonth, IntWritable> {
        private IntWritable delayMin = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                CSVParser csvParser = new CSVParser();
                String[] fields = csvParser.parseLine(value.toString());

                // Get necessary fields
                String year = fields[0];
                String month = fields[2];
                String flightDateStr = fields[5];
                String carrier = fields[6];
                String arrDelayMinutes = fields[37];
                String cancelled = fields[41];

                // Attributes needed for this computation are not missing
                if (carrier.isEmpty() || flightDateStr.isEmpty() || cancelled.equals("1.00")) {
                    return;
                }

                // Filter out the data that is not in the year 2008
                if (year.equals("2008") && !month.isEmpty() && !arrDelayMinutes.isEmpty() && !arrDelayMinutes.equals("NA")) {
                    // Parse the arrDelayMinutesStr as a double and round to the nearest integer
                    double arrDelayMinutesDouble = Double.parseDouble(arrDelayMinutes);
                    int arrDelayMinute = (int) Math.round(arrDelayMinutesDouble);
                    delayMin.set(arrDelayMinute);

                    CarrierMonth carrierMonth = new CarrierMonth(carrier, month);
                    context.write(carrierMonth, delayMin);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Make a composite key writable for the secondary sort
    public static class CarrierMonth implements WritableComparable<CarrierMonth> {
        public Text carrier;
        public Text month;

        public CarrierMonth() {
            this.carrier = new Text("");
            this.month = new Text("");
        }

        public CarrierMonth(String carrier, String month) {
            this.carrier = new Text(carrier.trim());
            this.month = new Text(month.trim());
        }

        public void setCarrier(String airLineName) {
            this.carrier.set(airLineName.getBytes());
        }

        public void setMonth(String month) {
            this.month.set(month.getBytes());
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(carrier.toString());
            dataOutput.writeUTF(month.toString());
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            carrier = new Text(dataInput.readUTF());
            month = new Text(dataInput.readUTF());
        }

        // Implement the compareTo method for the secondary sort
        @Override
        public int compareTo(CarrierMonth carrierMonth) {
            String thisCarrier = this.carrier.toString();
            String otherCarrier = carrierMonth.carrier.toString();
            int cmp = 0;
            if (thisCarrier.equals(otherCarrier)) {
                cmp = 0;
            } else if (thisCarrier.compareTo(otherCarrier) < 0) {
                cmp = -1;
            } else {
                cmp = 1;
            }
            if (cmp == 0) {
                int thisMonth = Integer.parseInt(this.month.toString());
                int otherMonth = Integer.parseInt(carrierMonth.month.toString());
                if (thisMonth == otherMonth) {
                    return 0;
                } else if (thisMonth < otherMonth) {
                    return -1;
                } else {
                    return 1;
                }
            }
            return cmp;
        }

        public Text getCarrier() {
            return carrier;
        }

        public Text getMonth() {
            return month;
        }

        public String toString() {
            return carrier.toString() + " " + month.toString();
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((carrier == null) ? 0 : carrier.hashCode());
            result = prime * result + ((month == null) ? 0 : month.hashCode());
            return result;
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null) return false;
            if (getClass() != o.getClass()) return false;
            CarrierMonth other = (CarrierMonth) o;
            if (carrier == null) {
                if (other.carrier != null) return false;
            } else if (!carrier.equals(other.carrier)) return false;
            if (month == null) {
                if (other.month != null) return false;
            } else if (!month.equals(other.month))
                return false;
            return true;
        }
    }


    // Reducer to aggregate the data for each month and carrier
    public static class MonthlyDelayReducer extends Reducer<CarrierMonth, IntWritable, CarrierMonth, Text> {
        private Text output = new Text();
        private HashMap<String, int[]> carrierDelayMap = new HashMap<>();

        public void reduce(CarrierMonth key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String carrier = key.getCarrier().toString().trim();
            String month = key.getMonth().toString().trim();

            int sumDelay = 0;
            int count = 0;
            for (IntWritable val : values) {
                sumDelay += val.get();
                count++;
            }
            int averageDelay =0;
            if (count > 0) {
                averageDelay = (int) Math.ceil((double) sumDelay / count);
            }
            // Check if the carrier exists in the map
            if (!carrierDelayMap.containsKey(carrier)) {
                // If not, create a new array for the carrier
                carrierDelayMap.put(carrier, new int[12]);
            }
            // Store the average delay for the month
            carrierDelayMap.get(carrier)[Integer.parseInt(month) - 1] = averageDelay;
        }

        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {

            try {
                for (String carrier : carrierDelayMap.keySet()) {

                    int[] monthlyDelays = carrierDelayMap.get(carrier);
                    StringBuilder outputLine = new StringBuilder();
                    for (int i = 0; i < monthlyDelays.length; i++) {
                        outputLine.append("(");
                        outputLine.append((i + 1)).append(",");
                        outputLine.append(monthlyDelays[i]).append("),");
                    }
                    outputLine.setLength(outputLine.length() - 1);
                    output.set(outputLine.toString());
                    context.write(new Text(carrier), output);
                }
            } catch (Exception e) {
                context.write(new Text("error"), new Text(e.getMessage()));
            }
        }
    }

    // Partition the data by the uniqueCarrier
    public static class CarrierPartitioner extends Partitioner<CarrierMonth, IntWritable> {
        @Override
        public int getPartition(CarrierMonth key, IntWritable value, int numPartitions) {
            try {
                // Use the hashcode() to help achieve better load balancing. multiply by 127 to perform some mixing
                return Math.abs(key.getCarrier().toString().hashCode() * 127) % numPartitions;
            } catch (Exception e) {
                throw new IllegalArgumentException("Error in partitioning", e);
            }
        }
    }

    // Comparator to sort the data by the carrier and month
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(CarrierMonth.class, true);
        }

        @Override
        public int compare(WritableComparable c1, WritableComparable c2) {
            CarrierMonth cm1 = (CarrierMonth) c1;
            CarrierMonth cm2 = (CarrierMonth) c2;
            return cm1.compareTo(cm2);
        }
    }

    // Comparator to group the data by the carrier & month
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(CarrierMonth.class, true);
        }

        @Override
        public int compare(WritableComparable c1, WritableComparable c2) {
            CarrierMonth cm1 = (CarrierMonth) c1;
            CarrierMonth cm2 = (CarrierMonth) c2;
            return cm1.compareTo(cm2);
        }
    }

    // Run the job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Monthly delay for each carrier in 2008");

        job.setJarByClass(hw4_SecondarySort.class);

        // Mapper
        job.setMapperClass(MonthlyDelayMapper.class);

        // Set partitioner and comparator
        job.setPartitionerClass(CarrierPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setReducerClass(MonthlyDelayReducer.class);
        // Set 6 reduce tasks
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(CarrierMonth.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //FileInputFormat.addInputPath(job, new Path("/Users/xiexiaoyang/hadoop/input/test.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("/Users/xiexiaoyang/hadoop/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}