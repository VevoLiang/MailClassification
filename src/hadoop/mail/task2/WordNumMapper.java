package hadoop.mail.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Vevo on 2017/7/16.
 */
public class WordNumMapper extends Mapper<Text, Text, Text, IntWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        for(String word:words){
            context.write(new Text(key.toString() + "," + word), new IntWritable(1));
        }
    }
}
