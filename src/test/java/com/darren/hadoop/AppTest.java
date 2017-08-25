package com.darren.hadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

    @Test
    public void testFS() throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.close();
    }
}
