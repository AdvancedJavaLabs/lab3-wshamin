package com.itmo.lab3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesAnalysisJob {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int argIndex = 0;
        if (args.length > 0 && (args[0].contains("SalesAnalysisJob") || args[0].startsWith("com.itmo"))) {
            argIndex = 1;
        }

        if (args.length - argIndex < 2) {
            System.err.println("Using: SalesAnalysisJob <input path> <output path> [num reducers]");
            System.exit(2);
        }
        
        String inputPath = args[argIndex];
        String outputPath = args[argIndex + 1];
        String intermediatePath = outputPath + "-temp";

        int numReducers = 1;
        
        if (args.length - argIndex >= 3) {
            try {
                numReducers = Integer.parseInt(args[argIndex + 2]);
                conf.setInt("mapreduce.job.reduces", numReducers);
            } catch (NumberFormatException e) {
                System.err.println("Error: number format exception for reducers: " + args[argIndex + 2]);
                System.exit(2);
            }
        }

        System.out.println("Launch Job 1: Data aggregation on category...");
        Job aggregationJob = Job.getInstance(conf, "Sales Analysis - Aggregation");
        aggregationJob.setJarByClass(SalesAnalysisJob.class);

        aggregationJob.setMapperClass(SalesMapper.class);
        aggregationJob.setMapOutputKeyClass(Text.class);
        aggregationJob.setMapOutputValueClass(Text.class);

        aggregationJob.setReducerClass(SalesReducer.class);
        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(aggregationJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(aggregationJob, new Path(intermediatePath));

        boolean success1 = aggregationJob.waitForCompletion(true);
        
        if (!success1) {
            System.err.println("Job 1 (aggregation) ends with error");
            System.exit(1);
        }
        
        System.out.println("Job 1 done");

        System.out.println("Launch Job 2: Sorting results on revenue...");

        try {
            FileSystem fs = FileSystem.get(conf);
            Path outputPathObj = new Path(outputPath);
            if (fs.exists(outputPathObj)) {
                fs.delete(outputPathObj, true);
                System.out.println("Existing output directory deleted: " + outputPath);
            }
        } catch (Exception e) {
            System.err.println("Warning: cant delete the output dir: " + e.getMessage());
        }
        
        Job sortJob = Job.getInstance(conf, "Sales Analysis - Sort");
        sortJob.setJarByClass(SalesAnalysisJob.class);

        sortJob.setMapperClass(SortMapper.class);
        sortJob.setMapOutputKeyClass(DoubleWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setReducerClass(SortReducer.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(sortJob, new Path(intermediatePath));
        FileOutputFormat.setOutputPath(sortJob, new Path(outputPath));

        sortJob.setNumReduceTasks(1);

        boolean success2 = sortJob.waitForCompletion(true);
        
        if (!success2) {
            System.err.println("Job 2 (sort) ends with error");
            System.exit(1);
        }
        
        System.out.println("Job 2 done");

        try {
            FileSystem fs = FileSystem.get(conf);
            Path tempPath = new Path(intermediatePath);
            if (fs.exists(tempPath)) {
                fs.delete(tempPath, true);
                System.out.println("Temp dir deleted: " + intermediatePath);
            }
        } catch (Exception e) {
            System.err.println("Warning: cant delete the directory: " + e.getMessage());
        }
        
        System.out.println("All jobs done. Results saved in: " + outputPath);
        
        System.exit(0);
    }
}

