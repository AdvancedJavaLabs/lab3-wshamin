package com.itmo.lab3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        
        if (line.isEmpty()) {
            return;
        }
        
        String[] fields = line.split(",");
        if (fields.length < 3) {
            return;
        }
        
        try {
            String category = fields[0].trim();
            double revenue = Double.parseDouble(fields[1].trim());
            String quantity = fields[2].trim();
            
            DoubleWritable sortKey = new DoubleWritable(-revenue);
            
            Text outputValue = new Text(String.format("%s,%.2f,%s", category, revenue, quantity));
            
            context.write(sortKey, outputValue);
            
        } catch (NumberFormatException e) {
            return;
        }
    }
}

