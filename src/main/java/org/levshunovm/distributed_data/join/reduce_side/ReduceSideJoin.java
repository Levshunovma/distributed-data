package org.levshunovm.distributed_data.join.reduce_side;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceSideJoin {
    public static Job configureJob(String... args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reduce-side join");
        job.setJarByClass(ReduceSideJoin.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WordMapper.FirstFile.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, WordMapper.SecondFile.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job;
    }
    
    public static void main(String[] args) throws Exception {
        Job job = configureJob(args[0], args[1], args[2]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
