package src;

import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class IntIntReducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
    private IntWritable finalCount = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : counts) {
            sum += value.get();
        }

        finalCount.set(sum);

        context.write(key, finalCount);
    }
}