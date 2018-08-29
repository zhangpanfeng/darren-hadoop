package com.darren.hadoop.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {

    public MyGroupComparator() {
        super(Text.class, true);
    }

    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        Text p1 = (Text) a;
        Text p2 = (Text) b;
        
        String[] array1 = p1.toString().split(" ");
        String[] array2 = p2.toString().split(" ");
        return array1[0].compareTo(array2[0]);

        // return 0;

    }
}
