package com.itmo.lab3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Главный класс для запуска MapReduce job анализа продаж.
 * Вычисляет общую выручку и количество проданных товаров по категориям.
 */
public class SalesAnalysisJob {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // При запуске через "hadoop jar", первый аргумент - имя класса
        // Определяем индекс начала реальных аргументов
        int argIndex = 0;
        if (args.length > 0 && (args[0].contains("SalesAnalysisJob") || args[0].startsWith("com.itmo"))) {
            argIndex = 1;
        }
        
        // Парсим аргументы командной строки
        if (args.length - argIndex < 2) {
            System.err.println("Usage: SalesAnalysisJob <input path> <output path> [num mappers] [num reducers]");
            System.exit(2);
        }
        
        String inputPath = args[argIndex];
        String outputPath = args[argIndex + 1];
        
        // Настройка числа mappers и reducers (опционально)
        if (args.length - argIndex >= 3) {
            try {
                int numMappers = Integer.parseInt(args[argIndex + 2]);
                conf.setInt("mapreduce.job.maps", numMappers);
            } catch (NumberFormatException e) {
                System.err.println("Ошибка: неверный формат числа mappers: " + args[argIndex + 2]);
                System.exit(2);
            }
        }
        
        if (args.length - argIndex >= 4) {
            try {
                int numReducers = Integer.parseInt(args[argIndex + 3]);
                conf.setInt("mapreduce.job.reduces", numReducers);
            } catch (NumberFormatException e) {
                System.err.println("Ошибка: неверный формат числа reducers: " + args[argIndex + 3]);
                System.exit(2);
            }
        }
        
        // Создаем Job
        Job job = Job.getInstance(conf, "Sales Analysis");
        job.setJarByClass(SalesAnalysisJob.class);
        
        // Настраиваем Mapper
        job.setMapperClass(SalesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        // Настраиваем Reducer
        job.setReducerClass(SalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        // Настраиваем входные и выходные пути
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        // Запускаем job и ждем завершения
        boolean success = job.waitForCompletion(true);
        
        System.exit(success ? 0 : 1);
    }
}

