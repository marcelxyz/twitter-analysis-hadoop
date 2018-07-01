package src;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TextIntReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>
{
    private IntWritable finalCount = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : counts) {
            sum += value.get();
        }

        finalCount.set(sum);

        context.write(key, finalCount);
    }
}