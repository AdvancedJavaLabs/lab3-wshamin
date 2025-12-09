package com.itmo.lab3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SalesReducer extends Reducer<Text, Text, Text, NullWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        double totalRevenue = 0.0;
        int totalQuantity = 0;
        
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            String value = iterator.next().toString();
            String[] parts = value.split(":");
            
            if (parts.length == 2) {
                try {
                    double revenue = Double.parseDouble(parts[0]);
                    int quantity = Integer.parseInt(parts[1]);
                    
                    totalRevenue += revenue;
                    totalQuantity += quantity;
                } catch (NumberFormatException e) {
                    continue;
                }
            }
        }
        
        String result = String.format("%s,%.2f,%d", key.toString(), totalRevenue, totalQuantity);
        context.write(new Text(result), NullWritable.get());
    }
}

