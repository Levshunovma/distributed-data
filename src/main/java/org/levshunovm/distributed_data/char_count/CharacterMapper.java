package org.levshunovm.distributed_data.char_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.levshunovm.distributed_data.MapReduceUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CharacterMapper extends Mapper<Object, Text, Text, IntWritable> {
    protected static final int ALPHABET_LENGTH = 26;
    private static final List<Text> characters = initCharacters();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        line = line.replaceAll(MapReduceUtils.IGNORED_EXPRESSION, "");
        line = line.toLowerCase();
        int[] charactersCount = new int[ALPHABET_LENGTH];
        for (int i = 0, n = line.length(); i < n; i++) {
            charactersCount[line.charAt(i) - 'a']++;
        }
        for (int i = 0; i < ALPHABET_LENGTH; i++) {
            context.write(characters.get(i), new IntWritable(charactersCount[i]));
        }
    }
    
    private static List<Text> initCharacters() {
        List<Text> characters = new ArrayList<>();
        for (int i = 0; i < ALPHABET_LENGTH; i++) {
            characters.add(new Text(String.valueOf((char) ('a' + i))));
        }
        return characters;
    }
}
