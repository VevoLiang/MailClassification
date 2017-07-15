package hadoop.mail.task1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by Vevo on 2017/7/15.
 */
public class ExtractReducer extends Reducer<Text, Text, Text, Text> {
    private int kEigenvector;
    private HashSet<String> eigenvectorSet = new HashSet<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        kEigenvector = context.getConfiguration().getInt("kEigenvector", 0);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Double> wordAndIG = new HashMap<String, Double>();
        for(Text t:values){
            String str = t.toString();
            String word = str.substring(0, str.lastIndexOf(":"));
            double IGValue = Double.parseDouble(str.substring(str.lastIndexOf(":")+1, str.length()));
            wordAndIG.put(word, IGValue);
        }
        int loopCount = (kEigenvector < wordAndIG.size())? kEigenvector:wordAndIG.size();
        for(int i=0; i < loopCount; i++){
            String maxWord = null;
            double maxValue = 0.0;
            for(Map.Entry<String, Double> entry:wordAndIG.entrySet()){
                if(maxValue < entry.getValue()){
                    maxWord = entry.getKey();
                    maxValue = entry.getValue();
                }
            }
            wordAndIG.remove(maxWord);
            eigenvectorSet.add(maxWord);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int i = 0;
        for(String str:eigenvectorSet){
            context.write(new Text(str), new Text(i++ + ""));
        }
        super.cleanup(context);
    }
}
