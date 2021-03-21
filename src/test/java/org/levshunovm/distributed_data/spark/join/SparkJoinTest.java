package org.levshunovm.distributed_data.spark.join;

import junit.framework.Assert;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class SparkJoinTest {

    private JavaSparkContext sparkContext;
    
    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("junit");
        sparkContext = new JavaSparkContext(conf);
    }

    @Test
    public void testFindResult() {
        List<String> file1 = new ArrayList<>();
        List<String> file2 = new ArrayList<>();
        Set<String> expected = new HashSet<>();
        
        file1.add("Both");
        file2.add("Both");
        
        file1.add("Left");
        file2.add("Right");
        expected.add("left");
        
        file1.add("lowercase");
        file2.add("LOWERCASE");
        
        file1.add("forgot,space");
        file2.add("forgot");
        expected.add("space");
        
        file1.add("Some sentence with few words.");
        file2.add("Another sentence, just for test.");
        expected.addAll(Arrays.asList("some", "with", "few", "words"));

        JavaRDD<String> file1RDD = sparkContext.parallelize(file1);
        JavaRDD<String> file2RDD = sparkContext.parallelize(file2);
        JavaRDD<String> actualRDD = SparkJoin.findResult(file1RDD, file2RDD);
        List<String> actual = actualRDD.collect();

        Assert.assertEquals(expected.size(), actual.size());
        int currentLength = Integer.MAX_VALUE;
        for (String word : actual) {
            int length = word.length();
            Assert.assertTrue(length <= currentLength);
            Assert.assertTrue(expected.contains(word));
            currentLength = length;
        }
    }
}
