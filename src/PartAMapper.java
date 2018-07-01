package src;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Optional;

public class PartAMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, IntWritable>
{
    private static final int AGGREGATION_INTERVAL = 5;

    private final IntWritable one = new IntWritable(1);

    private IntWritable length = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        Optional opt = Tweet.fromCsvLine(value.toString());

        if (!opt.isPresent()) return;

        Tweet tweet = (Tweet)opt.get();

        int tweetLength = (int)tweet.getText().codePoints().count();
        int aggregatedLength = 1 + (tweetLength - 1) / AGGREGATION_INTERVAL;

        length.set(aggregatedLength);

        context.write(length, one);
    }
}