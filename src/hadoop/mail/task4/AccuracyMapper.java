package hadoop.mail.task4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccuracyMapper extends Mapper<Text, Text, Text, IntWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String originClass = key.toString().split(" ")[0];
        String predictClass = key.toString().split(" ")[1];
        if(originClass.equals(predictClass)){
            context.write(new Text("true"), new IntWritable(1));
        }else{
            context.write(new Text("false"), new IntWritable(1));
        }
    }
}
