package experiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class Step4 {

    public static boolean run(Configuration config, Map<String, String> paths) {

        try {

            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);

            job.setJobName("step4");

            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step4_Mapper.class);

            job.setReducerClass(Step4_Reducer.class);

            job.setMapOutputKeyClass(Text.class);

            job.setMapOutputValueClass(Text.class);

// FileInputFormat.addInputPath(job, new

// Path(paths.get("Step4Input")));

            FileInputFormat.setInputPaths(job,

                    new Path[] { new Path(paths.get("Step4Input1")),

                            new Path(paths.get("Step4Input2")) });

            Path outpath = new Path(paths.get("Step4Output"));

            if (fs.exists(outpath)) {

                fs.delete(outpath, true);

            }

            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);

            return f;

        } catch (Exception e) {

            e.printStackTrace();

        }

        return false;

    }

    static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;

        protected void setup(Context context) throws IOException,

                InterruptedException {

            FileSplit split = (FileSplit) context.getInputSplit();

            flag = split.getPath().getParent().getName();

            System.out.println(flag + "**********************");

        }

        protected void map(LongWritable key, Text value, Context context)

                throws IOException, InterruptedException {

            String[] tokens = Pattern.compile("[\t,]").split(value.toString());

            if (flag.equals("output3")) {

                String[] v1 = tokens[0].split(":");

                String itemID1 = v1[0];

                String itemID2 = v1[1];

                String num = tokens[1];

                Text k = new Text(itemID1);

                Text v = new Text("A:" + itemID2 + "," + num);// A:i109,1

                context.write(k, v);

            } else if (flag.equals("output2")) {

                String userID = tokens[0];//u13

                for (int i = 1; i < tokens.length; i++) {//i468:2,i446:3

                    String[] vector = tokens[i].split(":");

                    String itemID = vector[0];

                    String pref = vector[1];

                    Text k = new Text(itemID);

                    Text v = new Text("B:" + userID + "," + pref);

                    context.write(k, v);//key:i468   value: B:u401,2

                }

            }

        }

    }

    static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {

            Map<String, Integer> mapA = new HashMap<String, Integer>();

            Map<String, Integer> mapB = new HashMap<String, Integer>();

            for (Text line : values) {

                String val = line.toString();

                if (val.startsWith("A:")) {

//key:i100   value : A:i100,3

                    String[] kv = Pattern.compile("[\t,]").split(

                            val.substring(2));

                    try {

                        mapA.put(kv[0], Integer.parseInt(kv[1]));

                    } catch (Exception e) {

                        e.printStackTrace();

                    }

                } else if (val.startsWith("B:")) {

//key:i468   value: B:u401,2

                    String[] kv = Pattern.compile("[\t,]").split(

                            val.substring(2));

                    try {

                        mapB.put(kv[0], Integer.parseInt(kv[1]));

                    } catch (Exception e) {

                        e.printStackTrace();

                    }

                }

            }

            double result = 0;

            Iterator<String> iter = mapA.keySet().iterator();

            while (iter.hasNext()) {

                String mapk = iter.next();// itemID

                int num = mapA.get(mapk).intValue();

                Iterator<String> iterb = mapB.keySet().iterator();

                while (iterb.hasNext()) {

                    String mapkb = iterb.next();// userID

                    int pref = mapB.get(mapkb).intValue();

                    result = num * pref;

                    Text k = new Text(mapkb);

                    Text v = new Text(mapk + "," + result);

                    context.write(k, v);

                }

            }

        }

    }

}
