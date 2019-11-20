import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MainControl {
    private static PriorProbability priorProbability = new PriorProbability();

    //the main control function
    public static void main(String[] args) throws Exception {
//        System.out.println("intput (TextClassifier -h) for help information.");
//        String contents = "Welcome to Native Bayes classilfier!\n" +
//                "TextClassifier <-option> [input] [ouput]\n" +
//                "follow are options:\n" +
//                "-p\tcount PriorProbability\n";

//        CommandLine cli = null;
//        CommandLineParser cliParser = new DefaultParser();
//        HelpFormatter helpFormatter = new HelpFormatter();
//        Options options = new Options();
//        options.addOption("h", "help", false, "Print this usage information");
//        options.addOption("i", "input", true, "input directory path");
//        options.addOption("o","output",true, "output directory path");
//        options.addOption("p","PriorProbability", false, "Count the prior probability from train set");
//
//        try{
//            cli = cliParser.parse(options, args);
//        }
//        catch (ParseException e){
//            helpFormatter.printHelp("please input -h for help", options);
//            e.printStackTrace();
//        }
//
//        if(cli.hasOption("h")){
//            helpFormatter.printHelp("TextClassifier", options);
//            System.exit(0);
//        }
//        if(cli.hasOption("p") && cli.hasOption("i") && cli.hasOption("o")){
//            String[] argPriorProbability = new String[2];
//            argPriorProbability[0] = cli.getOptionValue("i");
//            argPriorProbability[1] = cli.getOptionValue("o");
//            //priorProbability.run(argPriorProbability);
//        }
        int res = ToolRunner.run(new Configuration(), new PriorProbability(), args);
        System.out.println("over prior task, exit status: " + String.valueOf(res));

        res = ToolRunner.run(new Configuration(), new ConditionalProbability(), args);
        System.out.println("over condition task, exit status: " + String.valueOf(res));

        res = ToolRunner.run(new Configuration(), new Prediction(), args);
        System.out.println("over prediction task, exit status: " + String.valueOf(res));

        res = ToolRunner.run(new Configuration(), new Evaluation(), args);
        System.out.println("over evaluation task, exit status: " + String.valueOf(res));
    }
}
