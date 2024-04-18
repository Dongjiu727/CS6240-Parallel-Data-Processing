package org.CS6240_project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TermFrequencyWritable implements WritableComparable<TermFrequencyWritable> {
    private Text term;
    private IntWritable frequency;

    public TermFrequencyWritable() {
        this.term = new Text();
        this.frequency = new IntWritable();
    }

    public TermFrequencyWritable(String term, int frequency) {
        this.term = new Text(term);
        this.frequency = new IntWritable(frequency);
    }

    public void set(String term, int frequency) {
        this.term.set(term);
        this.frequency.set(frequency);
    }

    public Text getTerm() {
        return term;
    }

    public IntWritable getFrequency() {
        return frequency;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        term.write(out);
        frequency.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term.readFields(in);
        frequency.readFields(in);
    }

    @Override
    public int compareTo(TermFrequencyWritable o) {
        // Compare by frequency and then term
        int cmp = frequency.compareTo(o.frequency);
        if (cmp != 0) {
            return cmp;
        }
        return term.compareTo(o.term);
    }

    // Override hashCode, equals, and toString as needed
}


