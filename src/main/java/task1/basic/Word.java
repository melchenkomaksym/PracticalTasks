package task1.basic;

import org.apache.hadoop.io.Text;

public class Word implements Comparable<Word> {

    private Text data;
    private int count;

    Word(Text data, int count) {
        this.data = data;
        this.count = count;
    }
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
