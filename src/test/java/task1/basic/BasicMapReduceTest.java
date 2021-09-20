package task1.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class BasicMapReduceTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        Map mapper = new Map();
        Reduce reducer = new Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "2019-04-23 17:13:52.155578,74492f56-59cd-4759-b357-9817285cc39e," +
                        "Calvin Klein jeans Calvin Klein dress red dress Klein watch"));
        mapDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"),
                new Text("Calvin Klein jeans Calvin Klein dress red dress Klein watch"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("Calvin"));
        values.add(new Text("Klein"));
        values.add(new Text("jeans"));
        values.add(new Text("Calvin"));
        values.add(new Text("Klein"));
        values.add(new Text("dress"));
        values.add(new Text("red"));
        values.add(new Text("dress"));
        values.add(new Text("Klein"));
        values.add(new Text("watch"));
        reduceDriver.withInput(new Text("74492f56-59cd-4759-b357-9817285cc39e"), values);
        reduceDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"),
                new Text("Klein dress Calvin"));
        reduceDriver.runTest();
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
