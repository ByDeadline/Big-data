package com.example.bigdata;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AvgSalary extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvgSalary(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AvgFifaSalary");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AvgSalaryMapper.class);
        job.setReducerClass(AvgSalaryReducer.class);
        job.setCombinerClass(AvgSalaryCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AvgSalaryMapper extends Mapper<LongWritable, Text, Text, SumCount> {
        private final Text leagueId = new Text();
        private final SumCount sumCount = new SumCount();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    String[] fields = line.split(";");

                    // Extract relevant fields
                    leagueId.set(fields[16]);
                    double wage = Double.parseDouble(fields[11]);
                    double age = Double.parseDouble(fields[12]);
                    double weight = Double.parseDouble(fields[15]);

                    // Exclude players over 100 kg
                    if (weight <= 100) {
                        sumCount.set(new DoubleWritable(wage), new DoubleWritable(age), new IntWritable(1));
                        context.write(leagueId, sumCount);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class AvgSalaryReducer extends Reducer<Text, SumCount, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            double totalSalary = 0.0;
            double totalAge = 0.0;
            int playerCount = 0;

            for (SumCount val : values) {
                totalSalary += val.getSalarySum().get();
                totalAge += val.getAgeSum().get();
                playerCount += val.getCount().get();
            }

            double avgSalary = totalSalary / playerCount;
            double avgAge = totalAge / playerCount;

            String result =String.format("%.2f", avgSalary) + "," + String.format("%.2f", avgAge) + "," + playerCount;
            context.write(key, new Text(result));

        }
    }

    public static class AvgSalaryCombiner extends Reducer<Text, SumCount, Text, SumCount> {
        private final SumCount sum = new SumCount();

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            double totalSalary = 0.0;
            double totalAge = 0.0;
            int playerCount = 0;

            for (SumCount val : values) {
                totalSalary += val.getSalarySum().get();
                totalAge += val.getAgeSum().get();
                playerCount += val.getCount().get();
            }

            sum.set(new DoubleWritable(totalSalary), new DoubleWritable(totalAge), new IntWritable(playerCount));
            context.write(key, sum);
        }
    }
}