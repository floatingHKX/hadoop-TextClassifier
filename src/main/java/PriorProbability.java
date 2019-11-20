import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PriorProbability extends Configured implements Tool {
    public static class CategoryMapper
            extends Mapper<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static Logger logger = LogManager.getLogger(CategoryMapper.class);

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            logger.info("=============log===========");
            logger.info("key:" + key);
//            while (itr.hasMoreTokens()) {
//                //word.set(itr.nextToken());
//                context.write(key, one);
//            }
//            Path path = ((FileSplit)context.getInputSplit()).getPath();
//            String fileName = path.getParent().getName();
            context.write(key, one);
        }
    }

    public static class CountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        //BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境。
        int res = ToolRunner.run(new Configuration(), new PriorProbability(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception{
        /*
        由于hadoop集群中master结点的名字是master，为了方便ssh连接因此设置HADOOP_USER_NAME为master
         */
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

        Path inputPath = new Path(args[0] + "/train");
        Path outputPath = new Path(args[1] + "/prior");

        FileSystem fileSystem = FileSystem.get(conf);

        if(!fileSystem.exists(inputPath)){
            System.out.println("the train Set file doesn't exist, please upload the train Set first");
            System.exit(-1);
        }
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exist, and it has been deleted");
        }

        Job job = Job.getInstance(conf, "Prior");

        job.setJarByClass(PriorProbability.class);
        job.setMapperClass(CategoryMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        Path[] paths = getChildPaths(inputPath, fileSystem);
        if(paths.length == 0){
            System.out.println("the path under inputpath is empty!");
            System.exit(-1);
        }
        WholeFileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static Path[] getChildPaths(Path path, FileSystem fileSystem) throws IOException{
        FileStatus[] fileStatus = fileSystem.listStatus(path);
        List<Path> pathList = new ArrayList<Path>();
        for(FileStatus fileStatus1 : fileStatus){
            if(fileStatus1.isDirectory()){
                pathList.add(fileStatus1.getPath());
            }
        }

        Path[] paths = new Path[pathList.size()];
        for(int i=0; i<pathList.size(); i++){
            paths[i] = pathList.get(i);
        }
        return paths;
    }

    public static class WholeFileInputFormat
            extends FileInputFormat<Text, Text>{
        @Override
        protected boolean isSplitable(JobContext context, Path file){
            return false;
        }

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException{
            WholeFileRecordReader reader = new WholeFileRecordReader();
            reader.initialize(split, context);
            return reader;
        }
    }

    public static class WholeFileRecordReader extends RecordReader<Text, Text>{
        private FileSplit fileSplit; //保存输入分片，他将被转换成一条（key，value）记录
        private Configuration conf;  //配置对象
        private Text key = new Text();  //key对象，内容为空
        private Text value = new Text();  //value对象，内容为空
        private boolean processed = false;  //布尔变量记录记录是否被处理过
        private static Logger logger = LogManager.getLogger(WholeFileRecordReader.class);

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException{
            this.fileSplit = (FileSplit) split;  //将输入分片强制转换成FileSplit
            this.conf = context.getConfiguration();  //从context中获取配置信息
            //this.key = new Text(this.fileSplit.getPath().getName());
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException{
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException{
            return value;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException{
            if(!processed){
                //byte[] contents = new byte[(int) fileSplit.getLength()];
                Path filepath = fileSplit.getPath();

                this.key = new Text(filepath.getParent().getName());

                logger.info("==========myinfo===========");
                logger.info("filename: "+filepath.getName());

//                FileSystem fs = filepath.getFileSystem(conf);
//                FSDataInputStream in = fs.open(filepath);

                //BufferedReader br = new BufferedReader(new InputStreamReader(in));
                //FileStatus[] fileStatus = fs.listStatus(filepath);

                //String line = "";
                //String total = "";

//                while ((line = br.readLine()) != null){
//                    total += line + "\n";
//                }
//                for(FileStatus fileStatus1 : fileStatus){
//                    total += fileStatus1.getPath().getName() + "\n";
//                }
//                br.close();
//                in.close();
//                fs.close();
                //System.out.println(total);

                //logger.info("contents: "+total);
                value = new Text(filepath.getName());

                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public float getProgress() throws IOException{
            return processed?1.0f:0.0f;
        }

        @Override
        public void close() throws IOException{
            //do nothing
        }
    }
}
