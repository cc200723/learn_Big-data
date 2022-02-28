package experiment;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedMapper extends	Mapper<Object, Text, Text, Text> {
    private Text keyInfo = new Text();
    private Text valueInfo = new Text();
    private FileSplit split;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        split = (FileSplit) context.getInputSplit();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            keyInfo.set(itr.nextToken() + ":" + split.getPath().getName().toString());
            valueInfo.set("1");
            context.write(keyInfo, valueInfo);
        }
    }
}