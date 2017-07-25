package hadoop.mail.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Vevo on 2017/7/16.
 */
public class VSMMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String docAndClass = key.toString().split(",")[0];
        String word = key.toString().split(",")[1];
        context.write(new Text(docAndClass), new Text(word + "," + value.toString()));
    }
}
