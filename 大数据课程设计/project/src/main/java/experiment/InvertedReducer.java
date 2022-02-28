package experiment;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    public void reducer(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileList = new String();
        for (Text value : values) {
            fileList += value.toString() + ";";
        }
        result.set(fileList);
        context.write(key, result);
    }
}