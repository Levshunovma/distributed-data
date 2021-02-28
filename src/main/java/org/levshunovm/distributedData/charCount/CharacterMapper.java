package org.levshunovm.distributedData.charCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CharacterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final String IGNORED_EXPRESSION = "[^a-zA-Z]";
    protected static final int ALPHABET_LENGTH = 26;
    private static final List<Text> characters = new ArrayList<>();
    private IntWritable result = new IntWritable();

    static {
        for (int i = 0; i < ALPHABET_LENGTH; i++) {
            characters.add(new Text(String.valueOf((char) ('a' + i))));
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        line = line.replaceAll(IGNORED_EXPRESSION, "");
        line = line.toLowerCase();
        int[] charactersCount = new int[ALPHABET_LENGTH];
        for (int i = 0, n = line.length(); i < n; i++) {
            charactersCount[line.charAt(i) - 'a']++;
        }
        for (int i = 0; i < ALPHABET_LENGTH; i++) {
            result.set(charactersCount[i]);
            context.write(characters.get(i), result);
        }
    }
}
