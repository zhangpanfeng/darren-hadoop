package com.darren.hadoop.transfer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class TransferParamReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOG = Logger.getLogger(TransferParamReducer.class);

    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        // 获取参数方式一
        Configuration conf = context.getConfiguration();
        String value1 = conf.get("key1");
        LOG.info("第一个参数： " + value1);

        // 获取参数方式二
        SimpleParameter param = DefaultStringifier.load(conf, "key2", SimpleParameter.class);
        LOG.info("第二个参数： " + param);
        
        String value = context.getConfiguration().get("key");
        
        LOG.info("map参数： " + value);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        LOG.info("reduce");
    }

}
