package ru.mephi.hw1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        Text ip = new Text(line.substring(0, line.indexOf(" - - ")));
        int i = line.indexOf("HTTP/1.1\" ") + 14;
        LongWritable size = new LongWritable(Long.parseLong(line.substring(
                i, line.indexOf(' ', i))));
        context.write(ip, size);
    }
}