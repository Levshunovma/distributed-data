package org.levshunovm.distributed_data.spark.join;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.levshunovm.distributed_data.MapReduceUtils;
import scala.Tuple2;

import java.util.Arrays;

public class SparkJoin {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ReduceSideJoin");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> file1 = sparkContext.textFile(args[0]);
        JavaRDD<String> file2 = sparkContext.textFile(args[1]);
        findResult(file1, file2).saveAsTextFile(args[2]);
    }
    
    protected static JavaRDD<String> findResult(JavaRDD<String> file1, JavaRDD<String> file2) {
        return wordCounts(file1).leftOuterJoin(wordCounts(file2))
                // (K, V) left join (K, W) -> (K, (V, W))
                .filter(pair -> !pair._2._2.isPresent())
                .map(pair -> pair._1)
                .sortBy(String::length, false, 1);
    }

    private static JavaPairRDD<String, Integer> wordCounts(JavaRDD<String> javaRDD) {
        return javaRDD
                .flatMap(string -> Arrays.stream(string.split(MapReduceUtils.IGNORED_EXPRESSION))
                        .map(String::toLowerCase)
                        .iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);
    }
}
