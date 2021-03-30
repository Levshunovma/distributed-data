package org.levshunovm.distributed_data.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.levshunovm.distributed_data.MapReduceUtils;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class SparkSqlJoin {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Join")
                .getOrCreate();

        Dataset<String> file1 = getWords(spark.read().textFile(args[0]));
        Dataset<String> file2 = getWords(spark.read().textFile(args[1]));

        file1.join(file2, file1.col("value").equalTo(file2.col("value")), "leftanti")
                .sort(length(col("value")).desc())
                .withColumn("utc_timestamp", lit(startTime))
                .write().partitionBy("utc_timestamp").mode(SaveMode.Append).text(args[2]);
    }

    public static Dataset<String> getWords(Dataset<String> file) {
        return file
                .flatMap(string -> Arrays.stream(string.split(MapReduceUtils.IGNORED_EXPRESSION))
                        .map(String::toLowerCase)
                        .iterator(), Encoders.STRING())
                .dropDuplicates();
    }
}
