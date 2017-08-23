package com.darren.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.darren.hadoop.util.HDFSUtil;

public class WordCount {
    private static final Logger LOG = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Input path: " + args[0]);
        LOG.info("Output path: " + args[1]);
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("wordcound");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCoundMapper.class);
        job.setReducerClass(WordCoundReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //set reduce number
        job.setNumReduceTasks(1);
        
        //delete the output path
        HDFSUtil.deleteHDFSFile(conf, args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        double s = (end - start) / 1000.0;
        double m = s / 60.0;
        double h = m / 60.0;

        LOG.info("Total Cost: [" + s + "] s");
        LOG.info("Total Cost: [" + m + "] m");
        LOG.info("Total Cost: [" + h + "] h");
    }
}
