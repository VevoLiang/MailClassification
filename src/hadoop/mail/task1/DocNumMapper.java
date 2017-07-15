package hadoop.mail.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Vevo on 2017/7/14.
 */
public class DocNumMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String classLabel = key.toString().split("#")[1];
        String docName = key.toString().split("#")[0];
        context.write(new Text(classLabel), new Text(docName));
    }
}
