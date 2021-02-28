package org.levshunovm.distributedData.charCount;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CharCountTest {
    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        final CharacterMapper mapper = new CharacterMapper();
        final IntSumReducer reducer = new IntSumReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapperWithSingleInputAndMultipleOutput() throws Exception {
        final LongWritable inputKey = new LongWritable(0);
        final Text inputValue = new Text("Hi! This is a great book.");
        Map<String, Integer> outputValues = new HashMap<>();
        outputValues.put("a", 2);
        outputValues.put("b", 1);
        outputValues.put("e", 1);
        outputValues.put("g", 1);
        outputValues.put("h", 2);
        outputValues.put("i", 3);
        outputValues.put("k", 1);
        outputValues.put("o", 2);
        outputValues.put("r", 1);
        outputValues.put("s", 2);
        outputValues.put("t", 2);
        List<Pair<Text, IntWritable>> output = makeOutput(outputValues);

        mapDriver.withInput(inputKey, inputValue);
        mapDriver.withAllOutput(output);
        mapDriver.runTest();
    }
    
    @Test
    public void testReducer() throws Exception {
        final Text key = new Text("a");
        final ImmutableList<IntWritable> inputValue = ImmutableList.of(new IntWritable(3), new IntWritable(2));
        final IntWritable outputValue = new IntWritable(5);
        
        reduceDriver.withInput(key, inputValue);
        reduceDriver.withOutput(key, outputValue);
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        List<Pair<Object, Text>> input = new ArrayList<>();
        input.add(new Pair<>(new LongWritable(0), new Text("Hi! This is a great book.")));
        input.add(new Pair<>(new LongWritable(1), new Text("Small")));
        Map<String, Integer> outputValues = new HashMap<>();
        outputValues.put("a", 3);
        outputValues.put("b", 1);
        outputValues.put("e", 1);
        outputValues.put("g", 1);
        outputValues.put("h", 2);
        outputValues.put("i", 3);
        outputValues.put("k", 1);
        outputValues.put("l", 2);
        outputValues.put("m", 1);
        outputValues.put("o", 2);
        outputValues.put("r", 1);
        outputValues.put("s", 3);
        outputValues.put("t", 2);
        List<Pair<Text, IntWritable>> output = makeOutput(outputValues);
        
        mapReduceDriver.withAll(input);
        mapReduceDriver.withAllOutput(output);
        mapReduceDriver.runTest();
    }

    private void addPair(List<Pair<Text, IntWritable>> output, String key, Integer value) {
        output.add(new Pair<>(new Text(key), new IntWritable(value)));
    }
    
    private List<Pair<Text, IntWritable>> makeOutput(Map<String, Integer> values) {
        List<Pair<Text, IntWritable>> output = new ArrayList<>();
        for (int i = 0; i < CharacterMapper.ALPHABET_LENGTH; i++) {
            String key = String.valueOf((char) ('a' + i));
            addPair(output, key, values.getOrDefault(key, 0)); 
        }
        return output;
    }
}
