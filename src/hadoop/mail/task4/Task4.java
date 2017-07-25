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
        String outPath = args[2];
        int k = Integer.parseInt(args[3]); //KNN中的K

        try {
            doKNN(trainPath, testPath, outPath, k);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void doKNN(String train, String test, String out, int k) throws IOException, ClassNotFoundException, InterruptedException {
        //Reduce最好只由一个Reducer完成
        Configuration conf = new Configuration();
        conf.setInt("k", k);
        Job KNNJob = Job.getInstance(conf, "KNN");
        KNNJob.addCacheFile(URI.create(train + "/part-r-00000"));
        KNNJob.setJarByClass(Task4.class);
        KNNJob.setMapperClass(KNNMapper.class);
        KNNJob.setReducerClass(KNNReducer.class);
//        KNNJob.setMapperClass(KNNMapper1.class);
//        KNNJob.setReducerClass(KNNReducer1.class);
        KNNJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KNNJob.setOutputFormatClass(TextOutputFormat.class);
        KNNJob.setMapOutputKeyClass(Text.class);
        KNNJob.setMapOutputValueClass(IntWritable.class);
        KNNJob.setOutputKeyClass(Text.class);
        KNNJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(KNNJob, new Path(test));
        FileOutputFormat.setOutputPath(KNNJob, new Path(out));
        if(!KNNJob.waitForCompletion(true)){
            throw new RuntimeException("KNN MapReduce FAILED");
        }
    }
}
