package task1.basic;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.PriorityQueue;

public class Reduce extends Reducer<Text, Text, Text, Text> {

    private final PriorityQueue<Word> queue = new PriorityQueue<>(Collections.reverseOrder());
    private static final int topWordsNumber = 3;

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder text = new StringBuilder();
        for (Text sentence : values) {
            text.append(sentence);
            if (values.iterator().hasNext()) {
                text.append(" ");
            }
        }

        String[] words = text.toString().split(" ");
        HashMap<String, Integer> wordsWithCounts = new HashMap<>();
        for (String word : words) {
            int count = wordsWithCounts.getOrDefault(word, 0);
            wordsWithCounts.put(word, count + 1);
        }

        for (var entry : wordsWithCounts.entrySet()) {
            if (queue.size() <= topWordsNumber || entry.getValue() > queue.peek().getCount()) {
                queue.add(new Word(new Text(entry.getKey()), entry.getValue()));
            }
        }

        StringBuilder results = new StringBuilder();

        int counter = topWordsNumber;
        while (queue.iterator().hasNext() && counter > 0) {
            results.append(Objects.requireNonNull(queue.poll()).getData());
            if (queue.iterator().hasNext() && counter > 1) {
                results.append(", ");
            }
            counter--;
        }

        queue.clear();

        context.write(key, new Text(results.toString()));
    }
}
