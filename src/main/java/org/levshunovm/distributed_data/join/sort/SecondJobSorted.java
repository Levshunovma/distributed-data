package org.levshunovm.distributed_data.join.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.levshunovm.distributed_data.join.map_side.MapSideJoin;
import org.levshunovm.distributed_data.join.reduce_side.ReduceSideJoin;

import java.io.IOException;
import java.util.Collections;

public class SecondJobSorted {
    public static Job configureSecondJob(String... args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort");
        job.setJarByClass(SecondJobSorted.class);
        job.setMapperClass(LengthMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(LengthReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void main(String[] args) throws IOException {
        Job firstJob = null;
        if ("reduceSide".equals(args[0])) {
            firstJob = ReduceSideJoin.configureJob(args[1], args[2], args[3]);
        } else if ("mapSide".equals(args[0])) {
            firstJob = MapSideJoin.configureJob(args[1], args[2], args[3]);
        }
        if (firstJob != null) {
            JobControl jobControl = new JobControl("joinSort");
            ControlledJob firstJobControlled = new ControlledJob(firstJob, null);
            ControlledJob secondJobControlled = new ControlledJob(configureSecondJob(args[3], args[4]),
                    Collections.singletonList(firstJobControlled));
            jobControl.addJob(firstJobControlled);
            jobControl.addJob(secondJobControlled);
            jobControl.run();
        } else {
            System.out.println("Class for first job is not configured");
        }
    }
}
