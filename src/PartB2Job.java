package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartB2Job
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2) {
            System.err.println("Usage: <tweetFilePath> <outputDir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("top.number", "10");

        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(1);
        job.setJarByClass(PartB2Job.class);
        job.setCombinerClass(TextIntReducer.class);

        ChainMapper.addMapper(job, PartB2HashtagCountMapper.class, Object.class, Text.class,
                Text.class, IntWritable.class, new Configuration(false));

        ChainReducer.setReducer(job, TextIntReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, new Configuration(false));

        ChainReducer.addMapper(job, TopNMapper.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, null);

        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.waitForCompletion(true);
    }
}