package src;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Optional;

public class PartB1Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, IntWritable>
{
    private final IntWritable one = new IntWritable(1);

    private IntWritable hour = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        Optional opt = Tweet.fromCsvLine(value.toString());

        if (!opt.isPresent()) return;

        Tweet tweet = (Tweet)opt.get();

        hour.set(tweet.getDate().getHour());

        context.write(hour, one);
    }
}