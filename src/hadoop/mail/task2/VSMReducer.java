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
    private HashMap<String, String> classIdMap = new HashMap<>();
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
        Path[] cachePath = context.getLocalCacheFiles();
        //先读取类别编号文件
        BufferedReader reader = new BufferedReader(new FileReader(cachePath[0].toString()));
        String lineStr = reader.readLine();
        while(lineStr!=null && !"".equals(lineStr)){
            classIdMap.put(lineStr.split("\\t")[0], lineStr.split("\\t")[1]);
            lineStr = reader.readLine();
        }
        reader.close();

        //再读取特征词文件
        reader = new BufferedReader(new FileReader(cachePath[1].toString()));
        lineStr = reader.readLine();
        while(lineStr!=null && !"".equals(lineStr)){
            String[] strs = lineStr.split("\\t");
            if(strs.length < 3){
                context.write(new Text(cachePath[1].toString()), new Text());
            }else{
                WordInfo wordInfo = new WordInfo(strs[1], Double.parseDouble(strs[2]));
                eigenvectorMap.put(strs[0], wordInfo);
            }
            lineStr = reader.readLine();
        }
        reader.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        String classLabel = key.toString().split("#")[1];
//        String classId = classIdMap.get(classLabel);
//        String word;
//        int wordNum = 0;
//        int totalNum = 0;
//        HashMap<String, Integer> wordNumMap = new HashMap<>();
//        double tf;
//        double tf_idf;
//        StringBuilder VSMStr = new StringBuilder();
//
//        for (Text value:values){
//            word = value.toString().split(",")[0];
//            wordNum = Integer.parseInt(value.toString().split(",")[1]);
//            wordNumMap.put(word, wordNum);
//            totalNum += wordNum;
//        }
//
//        for (Map.Entry<String, Integer> entry:wordNumMap.entrySet()){
//            word = entry.getKey();
//            tf = entry.getValue() / totalNum;
//            if(eigenvectorMap.containsKey(word)){
//                tf_idf = tf * eigenvectorMap.get(word).idf;
//                VSMStr.append(word).append(":").append(tf_idf).append(" ");
//            }
//        }
//        VSMStr.deleteCharAt(VSMStr.length()-1);
//        context.write(new Text(classId), new Text(VSMStr.toString()));
    }
}
