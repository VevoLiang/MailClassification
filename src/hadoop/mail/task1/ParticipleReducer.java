package hadoop.mail.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vevo on 2017/7/14.
 */
public class ParticipleReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder valueList = new StringBuilder();
        for (Text value:values){
            valueList.append(value.toString()).append(",");
        }
        valueList.deleteCharAt(valueList.length()-1);
        context.write(key, new Text(valueList.toString()));
    }
}
