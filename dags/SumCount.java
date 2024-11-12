package com.example.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

public class SumCount implements WritableComparable<SumCount> {

    private DoubleWritable salarySum;
    private DoubleWritable ageSum;
    private IntWritable count;

    public SumCount() {
        set(new DoubleWritable(0), new DoubleWritable(0), new IntWritable(0));
    }

    public SumCount(Double salary, Double age, Integer count) {
        set(new DoubleWritable(salary), new DoubleWritable(age), new IntWritable(count));
    }

    public void set(DoubleWritable salarySum, DoubleWritable ageSum, IntWritable count) {
        this.salarySum = salarySum;
        this.ageSum = ageSum;
        this.count = count;
    }

    public DoubleWritable getSalarySum() {
        return salarySum;
    }

    public DoubleWritable getAgeSum() {
        return ageSum;
    }

    public IntWritable getCount() {
        return count;
    }

    public void addSumCount(SumCount sumCount) {
        this.salarySum.set(this.salarySum.get() + sumCount.getSalarySum().get());
        this.ageSum.set(this.ageSum.get() + sumCount.getAgeSum().get());
        this.count.set(this.count.get() + sumCount.getCount().get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        salarySum.write(dataOutput);
        ageSum.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        salarySum.readFields(dataInput);
        ageSum.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public int compareTo(SumCount o) {
        int result = salarySum.compareTo(o.salarySum);
        if (result != 0) return result;
        result = ageSum.compareTo(o.ageSum);
        return result == 0 ? count.compareTo(o.count) : result;
    }
}
