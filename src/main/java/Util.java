import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Util {
    public static String argInputPath = "";
    public static String argOutputPath = "hdfs://master:9000/output";
    public static Path[] getChildPaths(Path path, FileSystem fileSystem) throws IOException {
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
}
