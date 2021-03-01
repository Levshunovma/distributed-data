package org.levshunovm.distributed_data.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordMapper {
    public static class FirstFile extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable zero = new IntWritable(0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            WordMapper.map(key, value, context, zero);
        }
    }
    
    public static class SecondFile extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            WordMapper.map(key, value, context, one);
        }
    }
    
    public static void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context, IntWritable number) 
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            context.write(new Text(itr.nextToken()), number);
        }
    }
}
