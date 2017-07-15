package hadoop.mail.task1;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Vevo on 2017/7/14.
 */
public class WordInClass2Mapper extends Mapper<Text, Text, Text, Text>{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String word = key.toString().split("#")[0];
        String classLabel = key.toString().split("#")[1];
//        该词出现在该类的文档数
        String num = value.toString();
        context.write(new Text(word), new Text(classLabel + ":" + num));
    }
}
