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
        if (isValid(line)) {
            context.write(parseIp(line), parseSize(line));
        } else {
            System.out.println("");
        }
    }

    private boolean isValid(String line) {
        boolean hasSize;
        try {
            parseSize(line);
            hasSize = true;
        } catch (Exception e) {
            hasSize = false;
        }
        return line.contains(" - - ") && hasSize;
    }

    private Text parseIp(String line) {
        return new Text(line.substring(0, line.indexOf(" - - ")));
    }

    private LongWritable parseSize(String line) {
        int i = line.indexOf("HTTP/1.1\" ") + 14;
        return new LongWritable(Long.parseLong(line.substring(
                i, line.indexOf(' ', i))));
    }
}