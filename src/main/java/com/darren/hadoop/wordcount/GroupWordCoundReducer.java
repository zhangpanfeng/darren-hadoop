package com.darren.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class GroupWordCoundReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOG = Logger.getLogger(GroupWordCoundReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //this.calculate(key, values);
        new GroupEntity().calculate(key, values);
    }
    
    private void calculate(Text key, Iterable<IntWritable> values){
        int count = 0;
        
        
        for (IntWritable value : values) {
            count++;
            LOG.info("in key = " + key + " count = " + count);
        }
        
        LOG.info("out key = " + key + " count = " + count);
        
        return;
        //context.write(key, new IntWritable(sum));
    }

}
