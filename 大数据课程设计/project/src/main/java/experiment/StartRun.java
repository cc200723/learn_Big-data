package experiment;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class StartRun {

    public static void main(String[] args) {

        Configuration config = new Configuration();

        config.set("fs.defaultFS", "hdfs://master:9000");

        config.set("mapreduce.map.memory.mb","2048");

        config.set("mapreduce.reduce.memory.mb","3072");

        Map<String, String> paths = new HashMap<String, String>();

        paths.put("Step1Input", "/root/experiment/datas/");

        paths.put("Step1Output", "/root/experiment/output/output1");

        paths.put("Step2Input", paths.get("Step1Output"));

        paths.put("Step2Output", "/root/experiment/output/output2");

        paths.put("Step3Input", paths.get("Step2Output"));

        paths.put("Step3Output", "/root/experiment/output/output3");

        paths.put("Step4Input1", paths.get("Step2Output"));

        paths.put("Step4Input2", paths.get("Step3Output"));

        paths.put("Step4Output", "/root/experiment/output/output4");

        paths.put("Step5Input", paths.get("Step4Output"));

        paths.put("Step5Output", "/root/experiment/output/output5");

        paths.put("Step6Input", paths.get("Step5Output"));

        paths.put("Step6Output", "/root/experiment/output/output6");

//Step1.run(config, paths);

//Step2.run(config, paths);

//Step3.run(config, paths);

//Step4.run(config, paths);

//Step5.run(config, paths);

Step6.run(config, paths);

    }

    public static Map<String, Integer> R = new HashMap<String, Integer>();

    static {

        R.put("click", 1);

        R.put("collect", 2);

        R.put("cart", 3);

        R.put("alipay", 4);

    }

}