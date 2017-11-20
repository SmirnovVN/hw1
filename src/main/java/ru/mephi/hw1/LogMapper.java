package ru.mephi.hw1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LogMapper extends Mapper<Object, Text, Text, LongWritable> {

    /**
     * Writes ip on request size to context on each log line
     * @param key line number
     * @param value line content
     * @param context mr context
     */
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        try {
            context.write(parseIp(line), parseSize(line));
            context.getCounter(Log.class.getName(), Log.VALID.name()).increment(1);
        } catch (Exception e) {
            context.getCounter(Log.class.getName(), Log.INVALID.name()).increment(1);
        }
    }

    /**
     * Request ip parsing
     * @param line - line to parse
     * @return ip address
     */
    private Text parseIp(String line) {
        return new Text(line.substring(0, line.indexOf(" - - ")));
    }

    /**
     * Request size parsing
     * @param line - line to parse
     * @return request size
     */
    private LongWritable parseSize(String line) {
        int i = line.indexOf("HTTP/1.1\" ") + 14;
        return new LongWritable(Long.parseLong(line.substring(
                i, line.indexOf(' ', i))));
    }
}