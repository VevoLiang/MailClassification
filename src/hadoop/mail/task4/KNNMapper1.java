package hadoop.mail.task4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by Vevo on 2017/7/23.
 */
public class KNNMapper1 extends Mapper<Text, Text, Text, Text> {
    private int k;
    private SimilarityHolder kClass;
    private LinkedList<String> trainData;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        k = context.getConfiguration().getInt("k",-1);
        Path[] trainPath = context.getLocalCacheFiles();
        BufferedReader reader = new BufferedReader(new FileReader(trainPath[0].toString()));
        trainData = new LinkedList<>();
        String str = reader.readLine();
        while(str!=null && !"".equals(str)){
            trainData.add(str);
            str = reader.readLine();
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (k > 0){
            kClass = new SimilarityHolder(k);
        }else {
            throw new RuntimeException("K cannot be less than or equal to 0");
        }
        String originClass = key.toString();
        String testVSM = value.toString();
        String trainClass;
        String trainVSM;
        double similarity = 0.0;
        String predictClass;
        if (!"0".equals(testVSM)){
            for (String trainStr:trainData){
                trainClass = trainStr.split("\t")[0];
                trainVSM = trainStr.split("\t")[1];
                similarity = getSimilarity(trainVSM, testVSM);
                kClass.insert(trainClass, similarity);
            }
            predictClass = kClass.predictClass();
        }else{
            predictClass = "-1";
        }

        context.write(new Text(originClass + " -> " + predictClass), new Text(kClass.toString()));
    }

    private double getSimilarity(String train, String test){
        if (test.equals("0")){
            return 0.0; //理论上达不到这里，以防万一
        }
        String[] trainVSM = train.split(" ");
        String[] testVSM = test.split(" ");
        String eigenvector;
        double vectorValue;
        double similarity = 0.0;
        HashMap<String, Double> VSMMap = new HashMap<>();

        for (String str:trainVSM){
            eigenvector = str.split(":")[0];
            vectorValue = Double.parseDouble(str.split(":")[1]);
            VSMMap.put(eigenvector, vectorValue);
        }
        for (String str:testVSM){
            eigenvector = str.split(":")[0];
            vectorValue = Double.parseDouble(str.split(":")[1]);
            if(VSMMap.containsKey(eigenvector)){
                double trainValue = VSMMap.get(eigenvector);
                VSMMap.put(eigenvector, trainValue-vectorValue);
            }
        }
        for(Map.Entry<String, Double> entry:VSMMap.entrySet()){
            eigenvector = entry.getKey();
            vectorValue = entry.getValue();
            VSMMap.put(eigenvector, Math.pow(vectorValue, 2));
        }

        for(Map.Entry<String, Double> entry:VSMMap.entrySet()){
            similarity += entry.getValue();
        }
        //如果觉得没必要可以去掉平方根
        similarity = 1/(1+Math.sqrt(similarity));

        return similarity;
    }
}
