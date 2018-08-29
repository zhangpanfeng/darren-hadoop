package com.darren.hadoop.wordcount.jython;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCoundReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOG = Logger.getLogger(WordCoundReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        
        
        for (IntWritable value : values) {
            int count = value.get();
            LOG.info("value.get() = " + count);
            sum += count;
        }
        LOG.info("key = " + key);
        
        return;
        //context.write(key, new IntWritable(sum));
    }

}
