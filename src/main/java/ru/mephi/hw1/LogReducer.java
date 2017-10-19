package ru.mephi.hw1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text,LongWritable,Text,Text> {

    /**
     * Writes ip on average request size and total request size to context on each ip address
     * @param key ip address
     * @param values request sizes
     * @param context mr context
     */
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws InterruptedException, IOException {
        long sum = 0;
        long count = 0;
        for (LongWritable val : values) {
            sum += val.get();
            count++;
        }
        context.write(key, new Text(sum/count + "," + sum));
    }
}
