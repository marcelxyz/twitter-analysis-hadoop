package src;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PartCMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>
{
    private final IntWritable one = new IntWritable(1);

    private Text text = new Text();

    private HashMap<String, List<String>> medalists = new HashMap<>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        Optional opt = Tweet.fromCsvLine(value.toString());

        if (!opt.isPresent()) return;

        Tweet tweet = (Tweet)opt.get();

        for (Map.Entry<String, List<String>> entry : medalists.entrySet()) {
            String name = entry.getKey();

            if (!tweet.getText().contains(name)) continue;

            if (context.getConfiguration().getBoolean("top.athletesOnly", false)) {
                text.set(name);
                context.write(text, one);
            } else {
                for (String sport : entry.getValue()) {
                    text.set(sport.trim());
                    context.write(text, one);
                }
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Path path = new Path(context.getConfiguration().get("csv.path"));
        FSDataInputStream inputStream = path.getFileSystem(context.getConfiguration()).open(path);

        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        while ((line = reader.readLine()) != null) {
            String[] columns = line.trim().split(context.getConfiguration().get("csv.delimiter"));
            if (columns.length > 7 && !columns[0].equals("id")) {
                medalists
                    .computeIfAbsent(columns[1].trim(), k -> new ArrayList<>())
                    .addAll(Arrays.asList(columns[7].split(" ")));
            }
        }
    }
}