package task1.basic;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

    private static final int userIdColumnIndex = 1;
    private static final int searchQueryColumnIndex = 2;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        String[] data = record.split(",");

        context.write(new Text(data[userIdColumnIndex]), new Text(data[searchQueryColumnIndex]));
    }
}
