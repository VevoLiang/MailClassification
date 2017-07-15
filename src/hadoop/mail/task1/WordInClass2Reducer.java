package hadoop.mail.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vevo on 2017/7/14.
 */
public class WordInClass2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder classVector = new StringBuilder();
//        出现该词的所有文档总数
        int docNum = 0;
        for(Text value:values){
            docNum += Integer.parseInt(value.toString().split(":")[1]);
            classVector.append(value.toString()).append(",");
        }
        classVector.deleteCharAt(classVector.length()-1);
        Text wordAndDocNum = new Text(key.toString() + ":" + docNum);
//        (word:docNum, (class:num, class:num, ...))
        context.write(wordAndDocNum, new Text(classVector.toString()));
    }
}
