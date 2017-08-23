package com.darren.hadoop.transfer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class TransferParamMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOG = Logger.getLogger(TransferParamMapper.class);

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        // 获取参数方式一：获取简单字符串
        Configuration conf = context.getConfiguration();
        String value1 = conf.get("key1");
        LOG.info("第一个参数： " + value1);

        // 获取参数方式二： 获取简单对象
        SimpleParameter param = DefaultStringifier.load(conf, "key2", SimpleParameter.class);
        LOG.info("第二个参数： " + param);

        // 获取参数方式三：获取对象数组
        SimpleParameter[] params = DefaultStringifier.loadArray(conf, "key3", SimpleParameter.class);
        for (SimpleParameter parameter : params) {
            LOG.info("第三个参数： " + parameter);
        }

        // 获取参数方式四：获取复杂Map
        MapWritable map = DefaultStringifier.load(conf, "key4", MapWritable.class);
        Set<Entry<Writable, Writable>> entrySet = map.entrySet();
        for (Entry<Writable, Writable> entry : entrySet) {
            LOG.info("第四个参数： " + entry.getKey().toString() + ": " + entry.getValue().toString());
        }

        // 获取参数方式五： 获取复杂对象
        String value5 = conf.get("key5");
        LOG.info("第五个参数： " + value5);
        ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(URLDecoder.decode(value5, "UTF-8").getBytes("ISO-8859-1")));
        ComplexParameter complexParameter = null;
        try {
            complexParameter = (ComplexParameter) objIn.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        LOG.info("第五个参数： " + complexParameter);
        
        LOG.info("第六个参数： " + conf.get("key6"));
        
        LOG.info("第七个参数： " + conf.get("key7"));
        
        
        // 获取参数方式六： 获取文件
        URI[] urls = DistributedCache.getCacheFiles(conf);
        Path[] paths = DistributedCache.getLocalCacheFiles(conf);
        LOG.info("第六个参数： " + urls[0]);
        LOG.info("第六个参数： " + paths[0]);
        LOG.info("path================= ");
        for (Path path : paths) {
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
            String line = null;
            while((line = reader.readLine()) != null){
                LOG.info("第六个参数： " + line);
            }
            reader.close();
        }
//        LOG.info("uri================= ");
//        for (URI uri : urls) {
//            BufferedReader reader = new BufferedReader(new FileReader(uri.toString()));
//            String line = null;
//            while((line = reader.readLine()) != null){
//                LOG.info("第六个参数： " + line);
//            }
//            reader.close();
//        }
        
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        LOG.info("map ================= " + value.toString());
        
        context.getConfiguration().set("key", value.toString());
    }

}
