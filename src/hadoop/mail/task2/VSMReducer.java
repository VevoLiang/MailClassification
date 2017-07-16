package hadoop.mail.task2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vevo on 2017/7/16.
 */
public class VSMReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, WordInfo> eigenvectorMap = new HashMap<>();

    class WordInfo {
        String id;
        double idf;

        public WordInfo(String id, double idf) {
            this.id = id;
            this.idf = idf;
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //读取特征词文件
        Path[] cachePath = context.getLocalCacheFiles();
        BufferedReader reader = new BufferedReader(new FileReader(cachePath[0].toString()));
        String lineStr = reader.readLine();
        while(lineStr!=null && !"".equals(lineStr)){
            String[] strs = lineStr.split("\\t");
            WordInfo wordInfo = new WordInfo(strs[1], Double.parseDouble(strs[2]));
            eigenvectorMap.put(strs[0], wordInfo);
            lineStr = reader.readLine();
        }
        reader.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String classId = key.toString().split("#")[1];
        String word;
        int wordNum = 0;
        int totalNum = 0;
        HashMap<String, Double> wordNumMap = new HashMap<>();
        double tf;
        double tf_idf;
        StringBuilder VSMStr = new StringBuilder();

        for (Text value:values){
            word = value.toString().split(",")[0];
            wordNum = Integer.parseInt(value.toString().split(",")[1]);
            wordNumMap.put(word, (double)wordNum);
            totalNum += wordNum;
        }

        for (Map.Entry<String, Double> entry:wordNumMap.entrySet()){
            word = entry.getKey();
            tf = entry.getValue() / totalNum;
            if(eigenvectorMap.containsKey(word)){
                tf_idf = tf * eigenvectorMap.get(word).idf;
                if(tf_idf != 0.0){
                    VSMStr.append(eigenvectorMap.get(word).id).append(":").append(tf_idf).append(" ");
                }
            }
        }
        VSMStr.deleteCharAt(VSMStr.length()-1);
        context.write(new Text(classId), new Text(VSMStr.toString()));
    }
}
