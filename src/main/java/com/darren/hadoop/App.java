package com.darren.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import com.darren.hadoop.transfer.ComplexParameter;
import com.darren.hadoop.transfer.SimpleParameter;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
        System.out.println( "Hello World!" );
        Text text1 = new Text("a");
        Text text2 = new Text("a");
        System.out.println(text1.equals(text2));
        
        MapWritable map = new MapWritable();
        SimpleParameter param = new SimpleParameter();
        param.setName("darren");
        param.setValue("DArren zhang");
        map.put(text1, param);
        
        System.out.println(map.get(text2));
        
        ComplexParameter complexParameter = new ComplexParameter();
        complexParameter.setName("Darren 张");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(out);
        objOut.writeObject(complexParameter);
        objOut.flush();
        objOut.close();
        String complexString = out.toString("ISO-8859-1");
        System.out.println(complexString.length());
        System.out.println(complexString);
        String encodValue = URLEncoder.encode(complexString, "ISO-8859-1");
        System.out.println(encodValue.length());
        System.out.println(encodValue);
        
        
        ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(complexString.getBytes("ISO-8859-1")));
        ComplexParameter complex = null;
        try {
            complex = (ComplexParameter) objIn.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("第五个参数： " + complex);
    }
}
