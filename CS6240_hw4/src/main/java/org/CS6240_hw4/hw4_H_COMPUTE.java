package org.CS6240_hw4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class hw4_H_COMPUTE {
    public static final String HTABLE_NAME = "FlightInfo";
    public static final String CF = "Flight";
    public static final String CF_MONTH = "month";
    public static final String CF_CANCELLED = "Cancelled";
    public static final String CF_Arr_DELAY_MIN = "arrDelayMinutes";

    public static class HComputeMapper extends TableMapper<hw4_SecondarySort.CarrierMonth, IntWritable> {

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            try {
                String[] hbaseRowKey = Bytes.toString(row.get()).split("_");
                String carrier = hbaseRowKey[1];
                String month = Bytes.toString(value.getValue(Bytes.toBytes(CF), Bytes.toBytes(CF_MONTH)));

                String delayStr = Bytes.toString(value.getValue(Bytes.toBytes(CF), Bytes.toBytes(CF_Arr_DELAY_MIN)));
                int delay = delayStr.isEmpty() ? 0 : (int) Float.parseFloat(delayStr);
                hw4_SecondarySort.CarrierMonth carrierMonthKey = new hw4_SecondarySort.CarrierMonth(carrier, month);
                context.write(carrierMonthKey, new IntWritable(delay));

            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error processing row: " + Bytes.toString(row.get()));
            }
        }
    }

    public static class HComputeReducer extends
            Reducer<hw4_SecondarySort.CarrierMonth, IntWritable, hw4_SecondarySort.CarrierMonth, Text> {
        private Text output = new Text();
        private HashMap<String, int[]> carrierDelayMap = new HashMap<>();

        public void reduce(hw4_SecondarySort.CarrierMonth key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
    public static class CarrierPartitioner extends Partitioner<hw4_SecondarySort.CarrierMonth, IntWritable> {
        @Override
        public int getPartition(hw4_SecondarySort.CarrierMonth key, IntWritable value, int numPartitions) {
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
            super(hw4_SecondarySort.CarrierMonth.class, true);
        }

        @Override
        public int compare(WritableComparable c1, WritableComparable c2) {
            hw4_SecondarySort.CarrierMonth cm1 = (hw4_SecondarySort.CarrierMonth) c1;
            hw4_SecondarySort.CarrierMonth cm2 = (hw4_SecondarySort.CarrierMonth) c2;
            return cm1.compareTo(cm2);
        }
    }

    // Comparator to group the data by the carrier & month
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(hw4_SecondarySort.CarrierMonth.class, true);
        }

        @Override
        public int compare(WritableComparable c1, WritableComparable c2) {
            hw4_SecondarySort.CarrierMonth cm1 = (hw4_SecondarySort.CarrierMonth) c1;
            hw4_SecondarySort.CarrierMonth cm2 = (hw4_SecondarySort.CarrierMonth) c2;
            return cm1.compareTo(cm2);
        }

    }
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        //Configuration conf = new Configuration();
        Configuration conf = HBaseConfiguration.create();
        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());

        String startRowKey = "2008-01-01_";
        String stopRowKey = "2009-01-01_"; // This assumes that the row key for 2009-01-01 will not exist and is a lexical successor of any 2008 date.

        Scan scan = new Scan();
        scan.setCaching(500);// 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);// Don't set to true for MR jobs

        scan.setStartRow(Bytes.toBytes(startRowKey));
        scan.setStopRow(Bytes.toBytes(stopRowKey));

        FilterList filterList = new FilterList(
                FilterList.Operator.MUST_PASS_ALL);

        SingleColumnValueFilter cancelledFilter = new SingleColumnValueFilter(
                Bytes.toBytes(CF_CANCELLED),
                Bytes.toBytes("Cancelled"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("0.00")
        );
        // Apply filter while retrieving data from HBase
        scan.setFilter(cancelledFilter);
        scan.setFilter(filterList);

        Job job = new Job(conf, "H_COMPUTE, Secondary Sort");
        job.setJarByClass(hw4_H_COMPUTE.class);

        job.setMapperClass(HComputeMapper.class);
        job.setReducerClass(HComputeReducer.class);

        job.setOutputKeyClass(hw4_SecondarySort.CarrierMonth.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(CarrierPartitioner.class);
        job.setNumReduceTasks(6);

        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        TableMapReduceUtil.initTableMapperJob(HTABLE_NAME, scan,
                HComputeMapper.class, hw4_SecondarySort.CarrierMonth.class, IntWritable.class, job);

        //FileOutputFormat.setOutputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


