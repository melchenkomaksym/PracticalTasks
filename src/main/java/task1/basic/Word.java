package task1.basic;

import org.apache.hadoop.io.Text;

public record Word(Text data, int count) implements Comparable<Word> {

    public Text getData() {
        return data;
    }

    public int getCount() {
        return count;
    }

    public int compareTo(Word word) {
        return this.count - word.getCount();
    }
}
