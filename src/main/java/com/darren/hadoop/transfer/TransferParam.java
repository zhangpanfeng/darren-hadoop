package com.darren.hadoop.transfer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.darren.hadoop.util.HDFSUtil;
import com.darren.hadoop.wordcount.WordCount;

public class TransferParam {
    private static final Logger LOG = Logger.getLogger(TransferParam.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        LOG.info("Input path: " + args[0]);
        LOG.info("Output path: " + args[1]);

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();

        // 传递参数方式一 ：简单字符串
        conf.set("key1", "value1");

        // 传递参数方式二 ：简单对象
        SimpleParameter param = new SimpleParameter();
        param.setName("name");
        param.setValue("Darren");
        DefaultStringifier.store(conf, param, "key2");

        // 传递参数方式三 ： 对象数组
        SimpleParameter param1 = new SimpleParameter();
        param1.setName("name1");
        param1.setValue("Darren1");
        List<SimpleParameter> list = new ArrayList<>();
        list.add(param);
        list.add(param1);

        DefaultStringifier.storeArray(conf, list.toArray(), "key3");

        // 传递参数方式四 ： 复杂Map

        MapWritable map = new MapWritable();
        map.put(new Text("param"), param);
        map.put(new Text("param1"), param1);
        DefaultStringifier.store(conf, map, "key4");

        // 传递参数方式五 ： 复杂对象, 复杂对象需要手动序列化
        ComplexParameter complexParameter = new ComplexParameter();
        complexParameter.setName("Darren");
        complexParameter.setComplexList(list);
        complexParameter.setSilpleParameter(param);
        Map<String, SimpleParameter> complexMap = new HashMap<>();
        complexMap.put("test", param);
        complexParameter.setComplexMap(complexMap);
        Map<String, String> simpleMap = new HashMap<>();
        simpleMap.put("name", "Darren");
        complexParameter.setSimpleMap(simpleMap);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(out);
        objOut.writeObject(complexParameter);
        objOut.flush();
        objOut.close();
        String complexString = out.toString("ISO-8859-1");
        conf.set("key5", URLEncoder.encode(complexString, "UTF-8"));

        // 传递参数方式六 ： 传递文件
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);

        Job job = new Job(conf);
        conf.set("key6", "test6");
        job.setJarByClass(WordCount.class);
        job.setJobName("TransferParam");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TransferParamMapper.class);
        job.setReducerClass(TransferParamReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set reduce number
        job.setNumReduceTasks(1);

        // delete the output path
        HDFSUtil.deleteHDFSFile(conf, args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        conf.set("key7", "test7");
        long end = System.currentTimeMillis();
        double s = (end - start) / 1000.0;
        double m = s / 60.0;
        double h = m / 60.0;

        LOG.info("Total Cost: [" + s + "] s");
        LOG.info("Total Cost: [" + m + "] m");
        LOG.info("Total Cost: [" + h + "] h");
    }
}
