package src;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TopNMapper extends org.apache.hadoop.mapreduce.Mapper<Text, IntWritable, Text, IntWritable>
{
    private int count = 0;
    private TreeMap<Integer, List<Text>> map = new TreeMap<>();

    @Override
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException
    {
        final int topNumber = context.getConfiguration().getInt("top.number", 0);

        map.computeIfAbsent(value.get(), k -> new ArrayList<>()).add(key);
        count++;

        if (count <= topNumber) return;

        List<Text> list = map.firstEntry().getValue();
        list.remove(list.size() - 1);
        count--;

        if (list.isEmpty()) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, List<Text>> entry : map.entrySet()) {
            for (Text key : entry.getValue()) {
                context.write(key, new IntWritable(entry.getKey()));
            }
        }
    }
}