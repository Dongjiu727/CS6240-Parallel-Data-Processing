package org.CS6240_hw4;


import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.opencsv.CSVParser;

public class hw4_H_POPULATE {
    public static final String HTABLE_NAME = "FlightInfo";
    public static final String CF = "Flight";
    public static Configuration conf = HBaseConfiguration.create();
    public static class HPMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
        private CSVParser csvParser = null;
        private Connection connection = null;
        private Table table = null;

        // Set up the connection and table for the mapper.
        // setup() is called once at the beginning of the job.
        protected void setup(Context context) throws IOException {
            this.csvParser = new CSVParser();
            Configuration config = HBaseConfiguration.create();
            this.connection = ConnectionFactory.createConnection(config);
            this.table = connection.getTable(TableName.valueOf(HTABLE_NAME));
        }

        // Map the input file to HBase.
        public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = this.csvParser.parseLine(value.toString());

            if (fields.length > 0) {
                // Define the indices for specific columns
                String year = fields[0];
                String month = fields[1];
                String arrDelayMinutes = fields[37];
                String cancelled = fields[41];


                // Create the row key
                String rowKey = fields[5] + "_" + fields[6] + "_" + fields[10] + "_" + fields[11];
                Put put = new Put(Bytes.toBytes(rowKey));

                // Iterate over fields and add them to the Put object
                for (int i = 0; i < fields.length; i++) {
                    if (i != 5 && i != 6 && i != 1 && i != 10 && i != 11) {
                        String columnName = "col" + i;
                        put.addColumn(Bytes.toBytes(CF), Bytes.toBytes(columnName), Bytes.toBytes(fields[i]));
                    }
                }

                // Add specific columns with their own names
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("Year"), Bytes.toBytes(year));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("month"), Bytes.toBytes(month));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("arrDelayMinutes"), Bytes.toBytes(arrDelayMinutes));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("Cancelled"), Bytes.toBytes(cancelled));

                context.write(null, put);
            }
        }


        // Close the connection and admin after execution.
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.table.close();
            this.connection.close();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());

        createTable(conf);

        Job job = Job.getInstance(conf, "H_POPULATE, Populate HBase table FlightInfo.");
        job.setJarByClass(hw4_H_POPULATE.class);
        job.setMapperClass(HPMapper.class);
        TableMapReduceUtil.initTableReducerJob(HTABLE_NAME, null, job);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileInputFormat.addInputPath(job, new Path("/Users/xiexiaoyang/hadoop/input/test.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Create the HBase table if it doesn't exist.
    public static void createTable(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(HTABLE_NAME);

            // Check if the table exists
            if (admin.tableExists(tableName)) {
                // Disable the table if it is enabled
                if (admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                // Delete the table
                admin.deleteTable(tableName);
            }

            // Create a new table with the specified column family
            HTableDescriptor ht = new HTableDescriptor(tableName);
            ht.addFamily(new HColumnDescriptor(CF));
            admin.createTable(ht);
        }
    }
}
