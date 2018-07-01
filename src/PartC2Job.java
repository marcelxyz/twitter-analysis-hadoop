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

public class PartC2Job
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 3) {
            System.err.println("Usage: <medalistFilePath> <tweetFilePath> <outputDir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("csv.path", args[0]);
        conf.set("csv.delimiter", ",");
        conf.set("top.athletesOnly", "false");
        conf.set("top.number", "20");

        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(1);
        job.setJarByClass(PartC2Job.class);
        job.setCombinerClass(TextIntReducer.class);

        ChainMapper.addMapper(job, PartCMapper.class, Object.class, Text.class,
                Text.class, IntWritable.class, new Configuration(false));

        ChainReducer.setReducer(job, TextIntReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, new Configuration(false));

        ChainReducer.addMapper(job, TopNMapper.class, Text.class, IntWritable.class,
                NullWritable.class, Text.class, null);

        Path outputPath = new Path(args[2]);
        FileInputFormat.setInputPaths(job, args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.waitForCompletion(true);
    }
}