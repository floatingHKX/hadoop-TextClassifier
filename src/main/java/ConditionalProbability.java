import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class ConditionalProbability extends Configured implements Tool {
    public static class ClassWordMapper
            extends Mapper<Object, Text, ClassWordMapWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static Logger logger = LogManager.getLogger(ClassWordMapper.class);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String str = value.toString();
//            StringTokenizer itr = new StringTokenizer(str);
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            String classname = path.getParent().getName();
            logger.info("=============log===========");
            logger.info("key:" + classname);
            logger.info("value:" + value.toString());
            ClassWordMapWritable tmp = new ClassWordMapWritable();
            tmp.put(new Text(classname), value);
            context.write(tmp , one);
            //Path path =
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                ClassWordMapWritable tmp = new ClassWordMapWritable();
//                tmp.put(key, word);
//                context.write(tmp, one);
//            }
//            Path path = ((FileSplit)context.getInputSplit()).getPath();
//            String fileName = path.getParent().getName();
//            context.write(key, one);
        }
    }

    public static class ClassWordReducer
            extends Reducer<ClassWordMapWritable, IntWritable, ClassWordMapWritable, IntWritable>{
        @Override
        protected void reduce(ClassWordMapWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


//
    @Override
    public int run(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "master");
        //Configuration conf = new Configuration();
        Configuration conf = getConf();
//        conf.set("mapreduce.app-submission.cross-platform", "true");
//        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapred.jar","F:\\programming project\\TextClassifier\\out\\artifacts\\TextClassifier\\TextClassifier.jar");
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: TextClassifier -p <in> [<in>...] <out>");
//            System.exit(2);
//        }

        Path inputPath = new Path(args[0]+"/train");
        Path outputPath = new Path(args[1]+"/condition");

        FileSystem fileSystem = FileSystem.get(conf);

        if(!fileSystem.exists(inputPath)){
            System.out.println("the train Set file doesn't exist, please upload the train Set first");
            System.exit(-1);
        }
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exist, and it has been deleted");
        }

        Job job = Job.getInstance(conf, "Conditional");

        job.setJarByClass(ConditionalProbability.class);
        job.setMapperClass(ClassWordMapper.class);
        job.setCombinerClass(ClassWordReducer.class);
        job.setReducerClass(ClassWordReducer.class);
        job.setOutputKeyClass(ClassWordMapWritable.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setInputFormatClass(WordInClassInputFormat.class);
        Path[] paths = Util.getChildPaths(inputPath, fileSystem);
        if(paths.length == 0){
            System.out.println("the path under inputpath is empty!");
            System.exit(-1);
        }
//        WordInClassInputFormat.setInputPaths(job, paths);
        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        //BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境。
        int res = ToolRunner.run(new Configuration(), new ConditionalProbability(), args);
        System.exit(res);
    }
}
