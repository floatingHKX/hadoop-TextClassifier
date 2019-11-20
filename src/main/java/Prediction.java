import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.mortbay.jetty.security.HTAccessHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Prediction  extends Configured implements Tool {
    private HashMap<String ,Double> prior;
    private HashMap<HashMap<String ,String> ,Double> condition;

    public static class PredictMapper
            extends Mapper<Text, Text, Text, MapWritable> {
        private HashMap<String ,Double> prior;
        private HashMap<HashMap<String ,String> ,Double> condition;
        private static Logger logger = LogManager.getLogger(PredictMapper.class);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(conf);
            logger.info("argoutputpath:"+Util.argOutputPath);
            prior = calPrior(new Path(Util.argOutputPath+"/prior/part-r-00000"), fileSystem);
            condition = calCondition(new Path(Util.argOutputPath+"/condition/part-r-00000"), fileSystem);
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            logger.info("=============log===========");
            logger.info("key:" + key);
            for(String category : prior.keySet()){  //对与该文件，遍历每一个分类，得到他们的概率
                double probability = conditionalProbabilityForClass(value.toString(), category, prior, condition);
                MapWritable mapWritable = new MapWritable();
                mapWritable.put(new Text(category), new DoubleWritable(probability));
                context.write(key, mapWritable);
            }
//            while (itr.hasMoreTokens()) {
//                //word.set(itr.nextToken());
//                context.write(key, one);
//            }
//            Path path = ((FileSplit)context.getInputSplit()).getPath();
//            String fileName = path.getParent().getName();
//            context.write(key, one);
        }
    }

    public static class PredictReducer
            extends Reducer<Text, MapWritable, Text, Text> {
        private static Logger logger = LogManager.getLogger(PredictMapper.class);
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException{
            String lastCategory = "";
            double max = -9999999.0;
            logger.info("cordkey:" + key.toString());
            for(MapWritable value : values){
                for(Writable writable : value.keySet()){
                    double tmp = Double.valueOf(value.get(writable).toString());
                    logger.info(tmp+"\n");
                    if(tmp > max){
                        max = tmp;
                        lastCategory = writable.toString();
                    }
                }
            }
            context.write(key, new Text(lastCategory));
        }
    }

    public static double conditionalProbabilityForClass(String content, String className, HashMap<String, Double> prior, HashMap<HashMap<String ,String>, Double> condition){
        HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
        StringTokenizer itr = new StringTokenizer(content);
        while(itr.hasMoreTokens()){
            String tmpWord = itr.nextToken();
            Integer cord = wordMap.get(tmpWord);
            wordMap.put(tmpWord, cord==null?1:++cord);
        }
        double res = Math.log10(prior.get(className));
        for(Map.Entry<String ,Integer> entry : wordMap.entrySet()){  //遍历该文件中所有的单词
            HashMap<String ,String> tmp = new HashMap<String, String>();
            tmp.put(className, entry.getKey());
            Double conditionValue = condition.get(tmp);  //查看该类型中该单词的概率
            if(conditionValue == null){
                //如果该类型中没有找到该单词
                tmp.put(className, null);
                conditionValue = condition.get(tmp);
            }
            double tmpWordCondition = Math.log10(conditionValue);
            res += tmpWordCondition * entry.getValue();
        }
        return res;
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

                this.key = new Text(filepath.getName());

                logger.info("==========myinfo===========");
                logger.info("filename: "+filepath.getName());

                FileSystem fs = filepath.getFileSystem(conf);
                FSDataInputStream in = fs.open(filepath);

                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                //FileStatus[] fileStatus = fs.listStatus(filepath);

                String line = "";
                String total = "";

                while ((line = br.readLine()) != null){
                    total += line + "\n";
                }
//                for(FileStatus fileStatus1 : fileStatus){
//                    total += fileStatus1.getPath().getName() + "\n";
//                }
                br.close();
                in.close();
                fs.close();
                //System.out.println(total);

                //logger.info("contents: "+total);
                //value = new Text(filepath.getName());
                value = new Text(total);

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

    /*
    计算先验概率，存入类成员prior中
     */
    public static HashMap<String, Double> calPrior(Path priorPath, FileSystem fileSystem) throws IOException {
        FSDataInputStream fs = fileSystem.open(priorPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String line = "";
        HashMap<String, Integer> categoryMap = new HashMap<String, Integer>();
        while((line = br.readLine()) != null){
            StringTokenizer itr = new StringTokenizer(line);
            String category = itr.nextToken();
            Integer value = Integer.valueOf(itr.nextToken());
            categoryMap.put(category ,value);
        }
        br.close();
        fs.close();
        int sum = 0;
        for(Map.Entry<String ,Integer> entry : categoryMap.entrySet()){
            sum += entry.getValue();
        }
        HashMap<String ,Double> res = new HashMap<String, Double>();
        for(Map.Entry<String ,Integer> entry : categoryMap.entrySet()){
            Double value = entry.getValue()*1.0/sum;
            res.put(entry.getKey(), value);
        }
        return res;
    }

    /*
    计算条件概率，存入condition中
     */
    public static HashMap<HashMap<String, String>, Double> calCondition(Path conditonPath, FileSystem fileSystem) throws IOException{
        FSDataInputStream fs = fileSystem.open(conditonPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String line = "";
        HashMap<HashMap<String, String>, Integer> categorywordMap = new HashMap<HashMap<String, String>, Integer>();
        HashMap<String, Integer> categoryMap = new HashMap<String, Integer>();
        //HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
        HashSet<String> wordSet = new HashSet<String>();
        while((line = br.readLine()) != null){
            StringTokenizer itr = new StringTokenizer(line);
            String category = itr.nextToken();  //分类名
            String word = itr.nextToken();  //单词
            Integer value = Integer.valueOf(itr.nextToken());  //数量
            HashMap<String, String> couple = new HashMap<String, String>();
            couple.put(category, word);
            categorywordMap.put(couple, value);

            Integer tmp = categoryMap.get(category);
            categoryMap.put(category, tmp==null?value:value+tmp);  //记录分类的总单词数
            //tmp = wordMap.get(word);
            //wordMap.put(word, tmp==null?value:value+tmp);
            wordSet.add(word);  //记录所有的单词
        }
        br.close();
        fs.close();

        //最后返回的结果res
        HashMap<HashMap<String, String>, Double> res = new HashMap<HashMap<String, String>, Double>();
        Iterator<String> wordIt = wordSet.iterator();
        while(wordIt.hasNext()){  //遍历每个单词
            String tmpWord = wordIt.next();  //对于每个单词，遍历所有的分类
            for (Map.Entry<String, Integer> entry: categoryMap.entrySet()){
                String category = entry.getKey();  //得到分类名
                int sum = entry.getValue();  //分类包含的单词总数
                HashMap<String, String> tmpkey = new HashMap<String, String>();
                tmpkey.put(category, tmpWord);
                Integer include = categorywordMap.get(tmpkey);  //查看该单词在分类中出现的次数
                if(include == null){
                    Double value = 1.0 / sum;
                    res.put(tmpkey, value);
                }
                else {
                    Double value = 1.0*include / sum;
                    res.put(tmpkey, value);
                }
            }
        }
        for(Map.Entry<String ,Integer> entry : categoryMap.entrySet()){
            String category = entry.getKey();  //得到分类名
            int sum = entry.getValue();  //分类包含的单词总数
            HashMap<String, String> tmp = new HashMap<String, String>();
            tmp.put(category, null);
            res.put(tmp, 1.0/sum);
        }

        return res;
    }

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

        Path inputPath = new Path(args[0]+"/test");
        Path priorPath = new Path(args[1]+"/prior");
        Path conditionPath = new Path(args[1]+"/condition");
        Path outputPath = new Path(args[1]+"/prediction");

        FileSystem fileSystem = FileSystem.get(conf);

        if(!fileSystem.exists(inputPath)){
            System.out.println("the test Set file doesn't exist, please upload the test Set first");
            System.exit(-1);
        }
        if(!fileSystem.exists(priorPath)){
            System.out.println("the prior probability file doesn't exist, please execute prior task first");
            System.exit(-1);
        }
        if(!fileSystem.exists(conditionPath)){
            System.out.println("the conditional probability file doesn't exist, please execute condition task first");
            System.exit(-1);
        }
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exist, and it has been deleted");
        }

        this.prior = calPrior(new Path(priorPath, "part-r-00000"), fileSystem);
        this.condition = calCondition(new Path(conditionPath, "part-r-00000"), fileSystem);


        Job job = Job.getInstance(conf, "Prediction");

        job.setJarByClass(Prediction.class);
        job.setMapperClass(PredictMapper.class);
        //job.setCombinerClass(PredictReducer.class);
        job.setReducerClass(PredictReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        Path[] paths = Util.getChildPaths(inputPath, fileSystem);
        if(paths.length == 0){
            System.out.println("the path under inputpath is empty!");
            System.exit(-1);
        }
        WholeFileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        Util.argInputPath = args[0];
        Util.argOutputPath = args[1];
        int res = ToolRunner.run(new Configuration(), new Prediction(), args);
        System.exit(res);
    }
}
