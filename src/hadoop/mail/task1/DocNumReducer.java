package hadoop.mail.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by Vevo on 2017/7/14.
 */
public class DocNumReducer extends Reducer<Text, Text, Text, Text> {
    private int classCount = 0;
    private int totalDocNum = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Text> docNameSet = new HashSet<Text>();
        for(Text value:values){
            docNameSet.add(value);
        }
        totalDocNum += docNameSet.size();
//        (class, docNum)
        context.write(key, new Text(classCount++ + "\t" + docNameSet.size()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Total"), new Text("" + totalDocNum));
        super.cleanup(context);
    }
}
