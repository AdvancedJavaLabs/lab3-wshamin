package com.itmo.lab3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final String HEADER = "transaction_id,product_id,category,price,quantity";
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        
        if (line.isEmpty() || line.equals(HEADER)) {
            return;
        }
        
        String[] fields = line.split(",");
        if (fields.length < 5) {
            return;
        }
        
        try {
            String category = fields[2].trim();
            double price = Double.parseDouble(fields[3].trim());
            int quantity = Integer.parseInt(fields[4].trim());
            
            double revenue = price * quantity;
            
            context.write(new Text(category), new Text(revenue + ":" + quantity));
            
        } catch (NumberFormatException e) {
            return;
        }
    }
}

