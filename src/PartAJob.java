package src;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;

public class PartAJob
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2) {
            System.err.println("Usage: <tweetFilePath> <outputDir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(1);
        job.setJarByClass(PartAJob.class);
        job.setMapperClass(PartAMapper.class);
        job.setReducerClass(IntIntReducer.class);
        job.setCombinerClass(IntIntReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.waitForCompletion(true);
    }
}