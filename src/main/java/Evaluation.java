import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Evaluation extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "master");
        Configuration conf = getConf();
        conf.set("mapred.jar","F:\\programming project\\TextClassifier\\out\\artifacts\\TextClassifier\\TextClassifier.jar");
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: TextClassifier -p <in> [<in>...] <out>");
//            System.exit(2);
//        }

        Path testPath = new Path(args[0]+"/test");
        Path predictPath = new Path(args[1]+"/prediction");
        Path outputPath = new Path(args[1]+"/evaluation");
        Path priorPath = new Path(args[1]+"/prior");

        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exist, and it has been deleted");
        }

//        Job job = Job.getInstance(conf, "Conditional");
//
//        job.setJarByClass(ConditionalProbability.class);
//        job.setMapperClass(ConditionalProbability.ClassWordMapper.class);
//        job.setCombinerClass(ConditionalProbability.ClassWordReducer.class);
//        job.setReducerClass(ConditionalProbability.ClassWordReducer.class);
//        job.setOutputKeyClass(ClassWordMapWritable.class);
//        job.setOutputValueClass(IntWritable.class);

//        job.setInputFormatClass(WordInClassInputFormat.class);
        HashMap<String, String> realRes = getCorrectCategory(testPath, fileSystem);
        HashMap<String, String> predictRes = getPredictCategory(new Path(predictPath,"part-r-00000"), fileSystem);
        HashSet<String> categorySet = getCategorySet(new Path(priorPath,"part-r-00000"), fileSystem);
        int TP = 0;
        int TN = 0;
        int FP = 0;
        int FN = 0;
        HashMap<String, List<Double>> eval = new HashMap<String, List<Double>>();
        for(String c : categorySet){
            for(Map.Entry<String, String> entry : predictRes.entrySet()){
                String filename = entry.getKey();
                String category = entry.getValue();

                String realCategory = realRes.get(filename);
                if(realCategory.equals(c) && category.equals(c))
                    TP ++;
                else if(realCategory.equals(c) && !category.equals(c))
                    FN ++;
                else if(!realCategory.equals(c) && category.equals(c))
                    FP ++;
                else if(!realCategory.equals(c) && !category.equals(c))
                    TN ++;
            }
            double P = TP*1.0/(TP+FP);
            double R = TP*1.0/(TP+FN);
            double F1 = 2.0*P*R/(P+R);
            List<Double> PRF1 = new ArrayList<Double>();
            PRF1.add(P);
            PRF1.add(R);
            PRF1.add(F1);
            eval.put(c, PRF1);
        }
        Double Precison = 0.0;
        Double Recall = 0.0;
        Double F1 = 0.0;
        for(Map.Entry<String, List<Double>> entry : eval.entrySet()){
            List<Double> value = entry.getValue();
            Precison += value.get(0);
            Recall += value.get(1);
            F1 += value.get(2);
        }
        System.out.println("average-Precison: " + Precison/eval.size());
        System.out.println("average-Recall: " + Recall/eval.size());
        System.out.println("average-F1: " + F1/eval.size());
//        WordInClassInputFormat.setInputPaths(job, paths);
//        FileInputFormat.setInputPaths(job, paths);
//        FileOutputFormat.setOutputPath(job, outputPath);
//
//        return job.waitForCompletion(true) ? 0 : 1;
        return 1;
    }

    private HashSet<String> getCategorySet(Path path, FileSystem fileSystem) throws IOException{
        FSDataInputStream fs = fileSystem.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String line = "";
        HashSet<String> res = new HashSet<String>();
        while((line = br.readLine()) != null){
            StringTokenizer itr = new StringTokenizer(line);
            String category = itr.nextToken();
            Integer value = Integer.valueOf(itr.nextToken());
            res.add(category);
        }
        br.close();
        fs.close();
        return res;
    }

    private HashMap<String, String> getPredictCategory(Path path, FileSystem fileSystem) throws IOException{
        HashMap<String, String> res = new HashMap<String, String>();
        FSDataInputStream fs = fileSystem.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String line = "";
        while((line = br.readLine()) != null){
            StringTokenizer itr = new StringTokenizer(line);
            String filename = itr.nextToken();
            String category = itr.nextToken();
            res.put(filename, category);
        }
        fs.close();
        br.close();
        return res;
    }

    private HashMap<String, String> getCorrectCategory(Path path, FileSystem fileSystem) throws IOException{
        HashMap<String, String> res = new HashMap<String, String>();
        FileStatus[] dirStatus = fileSystem.listStatus(path);
        for(FileStatus dirStatus1 : dirStatus){
            if(dirStatus1.isDirectory()){
                String category = dirStatus1.getPath().getName();  //分类名字
                FileStatus[] fileStatuses = fileSystem.listStatus(dirStatus1.getPath());
                for(FileStatus fileStatus : fileStatuses){
                    String filename = fileStatus.getPath().getName();
                    res.put(filename, category);
                }
            }
        }
        return res;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new Evaluation(), args);
        System.exit(res);
    }
}
