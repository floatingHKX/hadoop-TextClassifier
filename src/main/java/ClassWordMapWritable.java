import org.apache.hadoop.io.*;

import java.util.Iterator;

public class ClassWordMapWritable extends MapWritable implements WritableComparable<ClassWordMapWritable> {
    @Override
    public int compareTo(ClassWordMapWritable classWordMapWritable){
        if(classWordMapWritable == null)
            return 1;

        Writable key1 = this.keySet().iterator().next();
        Writable key2 = classWordMapWritable.keySet().iterator().next();

        if(key1.toString().equals(key2.toString())){
            Text value1 = (Text) this.get(key1);
            Text value2 = (Text) classWordMapWritable.get(key2);
            if(value2 == null)
                return 1;
            if(value1 == null)
                return -1;
            return value1.compareTo(value2);
        }
        else{
            return key1.toString().compareTo(key2.toString());
        }
    }

    @Override
    public String toString(){
        Iterator it = this.keySet().iterator();
        String contents = "";
        while(it.hasNext()){
            Writable key = (Writable) it.next();
            Text value = (Text) this.get(key);
            contents += key.toString() + " " + value.toString();
        }
        return contents;
    }
}