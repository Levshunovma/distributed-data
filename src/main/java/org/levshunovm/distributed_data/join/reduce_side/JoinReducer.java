package org.levshunovm.distributed_data.join.reduce_side;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int[] appear = new int[2];
        for (IntWritable value : values) {
            appear[value.get()]++;
        }
        if (appear[0] > 0 && appear[1] == 0) {
            context.write(key, new IntWritable(appear[0]));
        }
    }
}
