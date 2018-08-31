package com.darren.hadoop.wordcount.combiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class WordCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WordCount.class);
    private static final String INPUT_PATH = "test-in/wordcount";
    private static final String OUTPUT_PATH = "test-out/wordcount";

    public static void main(String[] args) throws Exception {
        int rtnStatus = -1;
        try {
            rtnStatus = ToolRunner.run(new Configuration(), new WordCount(), args);
        } catch (Exception e) {

            LOG.error("WordCount Driver", e);
        }

        LOG.info("WordCount Driver Status Code :" + rtnStatus);

        System.exit(rtnStatus);
    }

    @Override
    public int run(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCoundMapper.class);
        job.setReducerClass(WordCoundReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

         job.setCombinerClass(WordCoundCombiner.class);

        // set reduce number
        job.setNumReduceTasks(1);
        LOG.info(String.format("No of Reducers: %s", job.getNumReduceTasks()));

        // delete the output path
        HDFSUtil.deleteHDFSFile(conf, OUTPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.waitForCompletion(true);

        // close
        this.close();

        long end = System.currentTimeMillis();
        double s = (end - start) / 1000.0;
        double m = s / 60.0;
        double h = m / 60.0;

        LOG.info("Total Cost: [" + s + "] s");
        LOG.info("Total Cost: [" + m + "] m");
        LOG.info("Total Cost: [" + h + "] h");

        return 0;
    }

    private void close() throws IOException {
        FileSystem fileSystem = FileSystem.get(getConf());
        fileSystem.close();
    }
}
