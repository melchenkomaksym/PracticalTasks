package task1.advanced;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import task1.basic.Map;
import task1.basic.Reduce;

import java.io.IOException;

public class AdvancedMapReduceTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        task1.basic.Map mapper = new Map();
        Reduce reducer = new Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155578,74492f56-59cd-4759-b357-9817285cc39e," +
                        "Calvin Klein jeans Calvin Klein dress red dresses Klein watches"));
        mapDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"),
                new Text("Calvin Klein jeans Calvin Klein dress red dress Klein watch"));
        mapDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155578,74492f56-59cd-4759-b357-9817285cc39e," +
                        "Calvin Klein jeans Calvin Klein dress red dress Klein watch"));
        mapReduceDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"),
                new Text("Klein dress Calvin"));
        mapReduceDriver.runTest();
    }
}
