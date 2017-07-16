package hadoop.mail.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vevo on 2017/7/15.
 */
public class ExtractReducer extends Reducer<Text, Text, Text, Text> {
    private int kEigenvector;
    private HashMap<String, Double> eigenvectorMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        kEigenvector = context.getConfiguration().getInt("kEigenvector", 0);
    }

    class IGAndIDF{
        double IGValue;
        double idf;
        public IGAndIDF(double IGValue, double idf) {
            this.IGValue = IGValue;
            this.idf = idf;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, IGAndIDF> wordAndIG = new HashMap<>();
        for(Text t:values){
            String str = t.toString();
            String word = str.substring(0, str.lastIndexOf(":"));
            double IGValue = Double.parseDouble(str.substring(str.lastIndexOf(":")+1, str.lastIndexOf(",")));
            double idf = Double.parseDouble(str.substring(str.lastIndexOf(",")+1, str.length()));
            wordAndIG.put(word, new IGAndIDF(IGValue, idf));
        }
        int loopCount = (kEigenvector < wordAndIG.size())? kEigenvector:wordAndIG.size();
        for(int i=0; i < loopCount; i++){
            String maxWord = null;
            double maxValue = 0.0;
            double idf = 0.0;
            for(Map.Entry<String, IGAndIDF> entry:wordAndIG.entrySet()){
                if(maxValue < entry.getValue().IGValue){
                    maxWord = entry.getKey();
                    maxValue = entry.getValue().IGValue;
                    idf = entry.getValue().idf;
                }
            }
            wordAndIG.remove(maxWord);
            eigenvectorMap.put(maxWord, idf);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int i = 0;
        for(Map.Entry<String, Double> entry: eigenvectorMap.entrySet()){
            context.write(new Text(entry.getKey()), new Text(i++ + "\t" + entry.getValue()));
        }
        super.cleanup(context);
    }
}
