package hadoop.mail.task1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Vevo on 2017/7/15.
 */
public class ExtractMapper extends Mapper<Text, Text, Text, Text> {
    private HashMap<String, Integer> classDocNum;
    private HashMap<String, String> classIds;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        classDocNum = new HashMap<>();
        classIds = new HashMap<>();
        Path[] cacheFiles= context.getLocalCacheFiles();
        BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
        String lineStr = reader.readLine();
        while(lineStr!=null && !"".equals(lineStr)){
            String[] strs = lineStr.split("\\t");
            if(!"Total".equals(strs[0])){
                classDocNum.put(strs[0], Integer.parseInt(strs[2]));
                classIds.put(strs[0], strs[1]);
            }else{
                classDocNum.put(strs[0], Integer.parseInt(strs[1]));
            }
            lineStr = reader.readLine();
        }
        reader.close();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordInClazz = value.toString().split(","); //数组每个字符串为class:num
        String keyStr = key.toString();
        String word = keyStr.substring(0, keyStr.lastIndexOf(":"));
        int wordInDocNum = Integer.parseInt(keyStr.substring(keyStr.lastIndexOf(":")+1,keyStr.length()));
        int N11 = 0;
        int N10 = 0;
        int N01 = 0;
        int N00 = 0;
        double IGValue = 0.0;
        double idf = 0.0;
        for(String wordInClass:wordInClazz){
            String classLabel = wordInClass.split(":")[0];
            N11 = Integer.parseInt(wordInClass.split(":")[1]);
            N10 = wordInDocNum - N11;
            N01 = classDocNum.get(classLabel) - N11;
            N00 = classDocNum.get("Total") - wordInDocNum + N01;
            IGValue = Math.pow((N11*N00 - N10*N01), 2)/((N11 + N10)*(N01 + N00));
            idf = Math.log(classDocNum.get("Total") / (wordInDocNum+1));
            context.write(new Text(classIds.get(classLabel)),
                    new Text(word + ":" + IGValue + "," + idf));
        }
    }
}
