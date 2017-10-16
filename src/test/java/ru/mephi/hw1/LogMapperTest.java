package ru.mephi.hw1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class LogMapperTest {

    private static Text[] logs = {
            new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET " +
                    "/~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0" +
                    " (compatible; YandexImages/3.0; +http://yandex.com/bots)\""),
            new Text("ip1 - - [24/Apr/2011:04:18:54 -0400] \"GET /~strabal/grease/photo1/T97-4.jpg " +
                    "HTTP/1.1\" 200 6244 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""),
            new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 " +
                    "\"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" " +
                    "\"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""),
            new Text("ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 " +
                    "\"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) " +
                    "Gecko/20110319 Firefox/3.6.16\"")
    };

    private static Text[] ips = {
            new Text("ip1"),
            new Text("ip1"),
            new Text("ip2"),
            new Text("ip2"),
    };

    private static LongWritable[] sizes = {
            new LongWritable(40028),
            new LongWritable(6244),
            new LongWritable(14917),
            new LongWritable(390)
    };

    @Test
    @SuppressWarnings("unchecked")
    public void testWriteInContext() throws IOException, InterruptedException {
        LogMapper mapper = new LogMapper();
        Mapper.Context context = mock(Mapper.Context.class);
        mapper.map(new LongWritable(1), logs[0], context);
        verify(context, times(1)).write(any(Text.class), any(LongWritable.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWriteCorrectValues() throws IOException, InterruptedException {
        LogMapper mapper = new LogMapper();
        Mapper.Context context = mock(Mapper.Context.class);
        for (int i = 0; i < logs.length; i++) {
            mapper.map(new LongWritable(1), logs[i], context);
            verify(context).write(ips[i], sizes[i]);
        }
    }
}
