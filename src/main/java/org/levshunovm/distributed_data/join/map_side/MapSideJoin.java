package org.levshunovm.distributed_data.join.map_side;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapSideJoin {
    public static Job configureJob(String... args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Map-side join");
        job.setJarByClass(MapSideJoin.class);
        job.setMapperClass(JoinMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        try {
            job.addCacheFile(new URI(args[1]));
        } catch (URISyntaxException e) {
            System.out.println("File not added");
            System.exit(1);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job;
    }

    public static void main(String[] args) throws Exception {
        Job job = configureJob(args[0], args[1], args[2]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
