package hadoop.mail.task2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Vevo on 2017/7/16.
 */
public class WordNumMapper extends Mapper<Text, Text, Text, IntWritable> {
    private HashMap<String, String> classIdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //读取类别编号文件
        Path[] cachePath = context.getLocalCacheFiles();
        BufferedReader reader = new BufferedReader(new FileReader(cachePath[0].toString()));
        String lineStr = reader.readLine();
        while(lineStr!=null && !"".equals(lineStr)){
            classIdMap.put(lineStr.split("\\t")[0], lineStr.split("\\t")[1]);
            lineStr = reader.readLine();
        }
        reader.close();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String docName = key.toString().split("#")[0];
        String classId = classIdMap.get(key.toString().split("#")[1]);
        String[] words = value.toString().split(",");
        for(String word:words){
            context.write(new Text(docName + "#" + classId + "," + word), new IntWritable(1));
        }
    }
}
