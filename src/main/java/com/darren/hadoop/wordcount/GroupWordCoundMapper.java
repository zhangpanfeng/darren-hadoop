package com.darren.hadoop.wordcount;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class GroupWordCoundMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOG = Logger.getLogger(GroupWordCoundMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text word = new Text();
        IntWritable one = new IntWritable(1);
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            LOG.info("token.nextToken() = " + token);
            int prefix = new Random().nextInt(2) + 1;
            word.set(prefix + " " + token);
            context.write(word, one);
        }

    }

    
    public static void main(String[] args) {
        
        for(int i = 0; i < 10; i++){
            System.out.println(new Random().nextInt(2) + 1);
        }
    }
}
