package task1.advanced;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

    private final HashMap<String, String> synonyms = new HashMap<>();
    private static final int userIdColumnIndex = 1;
    private static final int searchQueryColumnIndex = 2;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        String[] data = record.split(",");

        context.write(new Text(data[userIdColumnIndex]), new Text(replaceSynonyms(data[searchQueryColumnIndex])));
    }

    private String replaceSynonyms(String text) {
        initSynonyms();

        for (java.util.Map.Entry<String, String> synonymsEntry: synonyms.entrySet()) {
            text = text.replaceAll(String.format("\\b%s\\b", synonymsEntry.getKey()), synonymsEntry.getValue());
        }

        return text;
    }

    private void initSynonyms() {
        String synonymsFilePath = "src/main/resources/synonyms.txt";
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(synonymsFilePath));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] synonymsPair = line.split(":");
                synonyms.put(synonymsPair[0], synonymsPair[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
