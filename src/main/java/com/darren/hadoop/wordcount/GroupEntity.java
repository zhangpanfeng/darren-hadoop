package com.darren.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class GroupEntity {
    private static final Logger LOG = Logger.getLogger(GroupEntity.class);

    public void calculate(Text key, Iterable<IntWritable> values) {
        int count = 0;

        for (IntWritable value : values) {
            count++;
            LOG.info("in key = " + key + " count = " + count);
        }

        LOG.info("out key = " + key + " count = " + count);
    }
}
