package hadoop.mail.task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Vevo on 2017/7/21.
 */
public class Task4 {
    //输入应该为：[Task2输出路径路径]  [Task3输出路径]  [预测结果输出路径]  [KNN中的k值]
    public static void main(String[] args) {
        String trainPath = args[0] + "/tf_idf";
        String testPath = args[1] + "/tf_idf";
        String predictPath = args[2] + "/predict";
        String accuracyPath = args[2] + "/accuracy";
        int k = Integer.parseInt(args[3]); //KNN中的K

        try {
            doKNN(trainPath, testPath, predictPath, k);
            getAccuracy(predictPath, accuracyPath);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void doKNN(String train, String test, String out, int k) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("k", k);
        Job KNNJob = Job.getInstance(conf, "KNN");
        KNNJob.addCacheFile(URI.create(train + "/part-r-00000"));
        KNNJob.setJarByClass(Task4.class);
        KNNJob.setMapperClass(KNNMapper.class);
        KNNJob.setReducerClass(KNNReducer.class);
        KNNJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KNNJob.setOutputFormatClass(TextOutputFormat.class);
        KNNJob.setOutputKeyClass(Text.class);
        KNNJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(KNNJob, new Path(test));
        FileOutputFormat.setOutputPath(KNNJob, new Path(out));
        if(!KNNJob.waitForCompletion(true)){
            throw new RuntimeException("KNN MapReduce FAILED");
        }
    }

    private static void getAccuracy(String predictResult, String out) throws IOException, ClassNotFoundException, InterruptedException {
        //Reduce最好只由一个Reducer完成
        Configuration conf = new Configuration();
        Job accuracyJob = Job.getInstance(conf, "Accuracy");
        accuracyJob.setJarByClass(Task4.class);
        accuracyJob.setMapperClass(AccuracyMapper.class);
        accuracyJob.setReducerClass(AccuracyReducer.class);
        accuracyJob.setInputFormatClass(KeyValueTextInputFormat.class);
        accuracyJob.setOutputFormatClass(TextOutputFormat.class);
        accuracyJob.setMapOutputKeyClass(Text.class);
        accuracyJob.setMapOutputValueClass(IntWritable.class);
        accuracyJob.setOutputKeyClass(Text.class);
        accuracyJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(accuracyJob, new Path(predictResult));
        FileOutputFormat.setOutputPath(accuracyJob, new Path(out));
        if(!accuracyJob.waitForCompletion(true)){
            throw new RuntimeException("Accuracy MapReduce FAILED");
        }
    }
}
