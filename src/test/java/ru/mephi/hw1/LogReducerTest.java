package ru.mephi.hw1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LogReducerTest {

    private static Text[] ips = {
            new Text("ip1"),
            new Text("ip2"),
    };

    private static List<List<LongWritable>> sizes = new ArrayList<>();

    private static List<Text> result = new ArrayList<>();

    @Before
    public void setUp() {
        sizes.add(new ArrayList<LongWritable>());
        sizes.add(new ArrayList<LongWritable>());
        sizes.get(0).add(new LongWritable(40028));
        sizes.get(0).add(new LongWritable(6244));
        sizes.get(1).add(new LongWritable(14917));
        sizes.get(1).add(new LongWritable(390));
        result.add(new Text((40028 + 6244)/2 + "," + new LongWritable(40028 + 6244)));
        result.add(new Text((14917 + 390)/2 + "," + new LongWritable(14917 + 390)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWriteInContext() throws IOException, InterruptedException {
        LogReducer reducer = new LogReducer();
        Reducer.Context context = mock(Reducer.Context.class);
        reducer.reduce(ips[0], sizes.get(0), context);
        verify(context, times(1)).write(any(Text.class), any(Text.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWriteCorrectValues() throws IOException, InterruptedException {
        LogReducer reducer = new LogReducer();
        Reducer.Context context = mock(Reducer.Context.class);
        for (int i = 0; i < ips.length; i++) {
            reducer.reduce(ips[i], sizes.get(i), context);
            verify(context).write(ips[i], result.get(i));
        }
    }
}
