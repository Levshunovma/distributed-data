package org.levshunovm.distributed_data.join.map_side;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.levshunovm.distributed_data.MapReduceUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class JoinMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private final Set<String> secondFileWords = new HashSet<>();

    public void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            Path filePath = new Path(cacheFiles[0].toString());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)))) {
                String line = "";
                while ((line = reader.readLine()) != null) {
                    StringTokenizer itr = new StringTokenizer(line);
                    while (itr.hasMoreTokens()) {
                        String token = itr.nextToken()
                                .replaceAll(MapReduceUtils.IGNORED_EXPRESSION, "")
                                .toLowerCase();
                        secondFileWords.add(token);
                    }
                }
            }
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken()
                    .replaceAll(MapReduceUtils.IGNORED_EXPRESSION, "")
                    .toLowerCase();
            if (!secondFileWords.contains(token)) {
                context.write(new Text(token), one);
            }
        }
    }
}
