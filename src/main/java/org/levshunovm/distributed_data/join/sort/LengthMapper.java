package org.levshunovm.distributed_data.join.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.levshunovm.distributed_data.MapReduceUtils;

import java.io.IOException;
import java.util.StringTokenizer;

public class LengthMapper extends Mapper<Object, Text, IntWritable, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken()
                    .replaceAll(MapReduceUtils.IGNORED_EXPRESSION, "");
            if (StringUtils.isNotBlank(token)) {
                context.write(new IntWritable(token.length()), new Text(token));
            }
        }
    }
}
