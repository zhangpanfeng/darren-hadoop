package com.darren.hadoop.wordcount;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.darren.hadoop.util.HDFSUtil;

public class WordCount2 extends Configured implements Tool{
    private static final Logger LOG = Logger.getLogger(WordCount2.class);

    public static void main(String[] args) throws Exception {
        LOG.info(String.format("mapred_reduce_tasks: %s", System.getProperty("mapred_reduce_tasks")));
        Properties properties = System.getProperties();
        for(Object key: properties.keySet()){
            LOG.info("[ " + key + " = " + properties.get(key) + " ]");
        }
        int rtnStatus = 2;
        try {

            rtnStatus = ToolRunner.run(new Configuration(), new WordCount2(), args);

        } catch (Exception e) {

            LOG.error("Caught Exception while running Generic Loader", e);
        }
        
        LOG.info("Return status code for Generic Loader :" + rtnStatus);

        System.exit(rtnStatus);
    }

    @Override
    public int run(String[] args) throws Exception {
        LOG.info("Input path: " + args[0]);
        LOG.info("Output path: " + args[1]);
        //System.setProperty("hadoop.home.dir", "C:/devprogram/hadoop");
        long start = System.currentTimeMillis();
        Configuration conf = getConf();
        LOG.info(String.format("No of Reducers: %s", conf.get("mapred.reduce.tasks")));
        LOG.info(String.format("No of Reducers: %s", conf.get("mapreduce.job.reduces")));
        LOG.info(String.format("No of Reducers: %s", conf.get("mapred_reduce_tasks")));
        LOG.info(String.format("No of Reducers: %s", System.getProperty("mapred_reduce_tasks")));
        Job job = new Job(conf);
        job.setJarByClass(WordCount2.class);
        job.setJobName("wordcound");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCoundMapper.class);
        job.setReducerClass(WordCoundReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //set reduce number
        job.setNumReduceTasks(1);
        LOG.info(String.format("No of Reducers: %s", job.getNumReduceTasks()));
        
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
        return 0;
    }
}
