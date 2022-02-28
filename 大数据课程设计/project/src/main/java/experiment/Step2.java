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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Step2 {

    public static boolean run(Configuration config,Map<String, String> paths){

        try {

            FileSystem fs =FileSystem.get(config);

            Job job =Job.getInstance(config);

            job.setJobName("step2");

            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step2_Mapper.class);

            job.setReducerClass(Step2_Reducer.class);

            job.setMapOutputKeyClass(Text.class);

            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));

            Path outpath=new Path(paths.get("Step2Output"));

            if(fs.exists(outpath)){

                fs.delete(outpath,true);

            }

            FileOutputFormat.setOutputPath(job, outpath);

            boolean f= job.waitForCompletion(true);

            return f;

        } catch (Exception e) {

            e.printStackTrace();

        }

        return false;

    }

    static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text>{

        protected void map(LongWritable key, Text value,

                           Context context)

                throws IOException, InterruptedException {

            String[]  tokens=value.toString().split(",");

            String item=tokens[0];

            String user=tokens[1];

            String action =tokens[2];

            Text k= new Text(user);

            Integer rv =StartRun.R.get(action);

            Text v =new Text(item+":"+ rv.intValue());

//value:i1:1

//value: i1:2

            context.write(k, v);

        }

    }

    static class Step2_Reducer extends Reducer<Text, Text, Text, Text>{

        protected void reduce(Text key, Iterable<Text> i,

                              Context context)

                throws IOException, InterruptedException {

            Map<String, Integer> r =new HashMap<String, Integer>();

            for(Text value :i){

                String[] vs =value.toString().split(":");//value: i1:2

                String item=vs[0];//i1

                Integer action=Integer.parseInt(vs[1]);

                action = ((Integer) (r.get(item)==null?  0:r.get(item))).intValue() + action;

                r.put(item,action);

            }

            StringBuffer sb =new StringBuffer();

            for(Entry<String, Integer> entry :r.entrySet() ){

                sb.append(entry.getKey()+":"+entry.getValue().intValue()+",");

            }

            context.write(key,new Text(sb.toString().substring(0,sb.toString().length()-1)));

        }

    }

}
