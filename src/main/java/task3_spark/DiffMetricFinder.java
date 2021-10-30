package task3_spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

public class DiffMetricFinder {

    public static void main(String[] args) {

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("version",  DataTypes.StringType, true),
                DataTypes.createStructField("datetime", DataTypes.StringType, true),
                DataTypes.createStructField("hostname", DataTypes.StringType, true),
                DataTypes.createStructField("uuid", DataTypes.StringType, true),
                DataTypes.createStructField("metric", DataTypes.DoubleType, true),
                DataTypes.createStructField("metrics", DataTypes.StringType, true)
        });

        StructType jsonSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("m1",  DataTypes.IntegerType, true),
                DataTypes.createStructField("m2", DataTypes.IntegerType, true),
                DataTypes.createStructField("m3", DataTypes.IntegerType, true),
                DataTypes.createStructField("m4", DataTypes.IntegerType, true),
                DataTypes.createStructField("m5", DataTypes.IntegerType, true)
        });

        SparkSession session = SparkSession.builder()
                .appName("DiffMetricFinder")
                .master("local")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .getOrCreate();

        Dataset<Row> testData = session.read().format("csv")
                .option("delimiter", "\t")
                .schema(schema)
                .load("file://home/ubuntu/testdata/v1/output.tsv");

        testData = testData.withColumn("metrics", functions.from_json(testData.col("metrics"), jsonSchema));

        Dataset<Row> testData2 = session.read().format("csv")
                .option("delimiter", "\t")
                .schema(schema)
                .load("file://home/ubuntu/testdata/v2/output.tsv");

        testData2 = testData2.withColumn("metrics", functions.from_json(testData2.col("metrics"), jsonSchema));

        testData.createOrReplaceTempView("testData");
        testData2.createOrReplaceTempView("testData2");

        String query = "select testData.uuid, testData.hostname, testData.metric, testData2.metric, testData.metrics, testData2.metrics " +
                "from testData " +
                "inner join testData2 " +
                "on testData.uuid = testData2.uuid " +
                "where abs(testData.metric - testData2.metric)*100/testData.metric > 10 " +
                "or abs(testData.metrics['m1'] - testData2.metrics['m1'])*100/testData.metrics['m1'] > 10 " +
                "or abs(testData.metrics['m2'] - testData2.metrics['m2'])*100/testData.metrics['m2'] > 10 " +
                "or abs(testData.metrics['m3'] - testData2.metrics['m3'])*100/testData.metrics['m3'] > 10 " +
                "or abs(testData.metrics['m4'] - testData2.metrics['m4'])*100/testData.metrics['m4'] > 10 " +
                "or abs(testData.metrics['m5'] - testData2.metrics['m5'])*100/testData.metrics['m5'] > 10";

        Dataset<Row> res = session.sql(query);

        res.show();
    }
}
