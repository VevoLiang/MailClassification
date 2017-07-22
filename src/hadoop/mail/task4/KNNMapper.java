package hadoop.mail.task4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Vevo on 2017/7/21.
 */
public class KNNMapper extends Mapper<Text, Text, Text, IntWritable> {
    private BufferedReader reader;
    private DistanceHolder kClass;
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Path[] trainPath = context.getLocalCacheFiles();
        reader = new BufferedReader(new FileReader(trainPath[0].toString()));
        int k = context.getConfiguration().getInt("k",-1);
        if (k > 0){
            kClass = new DistanceHolder(k);
        }else {
            throw new RuntimeException("K cannot be less than or equal to 0");
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String testVSM = value.toString().split("\t")[1];
        String trainStr = reader.readLine();
        String trainClass;
        String trainVSM;
        double distance = 0.0;
        while(trainStr != null && !"".equals(trainStr)){
            trainClass = trainStr.split("\t")[0];
            trainVSM = trainStr.split("\t")[1];
            distance = DistanceUtil.getDistance(trainVSM, testVSM);
            kClass.insert(trainClass, distance);
            trainStr = reader.readLine();
        }
        String predictClass = kClass.predictClass();
        String originClass = value.toString().split("\t")[0];
        if (predictClass.equals(originClass)){
            context.write(new Text("true"), new IntWritable(1));
        }else{
            context.write(new Text("false"), new IntWritable(1));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        reader.close();
        super.cleanup(context);
    }
}
