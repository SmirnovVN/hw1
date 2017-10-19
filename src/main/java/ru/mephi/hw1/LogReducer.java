package ru.mephi.hw1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text,LongWritable,Text,Pair<FloatWritable, LongWritable>> {
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws InterruptedException, IOException {
        long sum = 0;
        long count = 0;
        for (LongWritable val : values) {
            sum += val.get();
            count++;
        }
        context.write(key, new Pair<>(new FloatWritable(sum/count), new LongWritable(sum)));
    }
}
