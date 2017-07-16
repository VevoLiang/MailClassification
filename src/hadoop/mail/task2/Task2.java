package hadoop.mail.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Vevo on 2017/7/16.
 */
public class Task2 {
    //完整的类别编号文件路径
    private static Path classIdPath;
    private static Path eigenvectorPath;
    private static int reduceNum;

    public static void main(String[] args) {
        //输入应该为：task1输出目录 task2输出目录 reduce数量
        classIdPath = new Path(args[0] + "/doc_num/part-r-00000");
        eigenvectorPath = new Path(args[0] + "/eigenvector/part-r-00000");
        reduceNum = Integer.parseInt(args[2]);
        //输入路径应该是分词后的文档路径
        String in = args[0] + "/part";
        String out = args[1];

        try {
            getTF(in, out);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static String getTF(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf1 = new Configuration();
//        Job wordNumJob = Job.getInstance(conf1, "WordNumInDoc");
//        wordNumJob.setJarByClass(Task2.class);
//        wordNumJob.setInputFormatClass(KeyValueTextInputFormat.class);
//        wordNumJob.setOutputFormatClass(TextOutputFormat.class);
//        wordNumJob.setMapperClass(WordNumMapper.class);
//        wordNumJob.setReducerClass(WordNumReducer.class);
//        wordNumJob.setOutputKeyClass(Text.class);
//        wordNumJob.setOutputValueClass(IntWritable.class);
//        wordNumJob.setNumReduceTasks(reduceNum);
//        FileInputFormat.addInputPath(wordNumJob, new Path(inputPath));
        String wordNumPath = outputPath + "/word_num_in_doc";
//        FileOutputFormat.setOutputPath(wordNumJob, new Path(wordNumPath));
//        wordNumJob.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job VSMJob = Job.getInstance(conf2, "TF-IDF/VSM");
        //顺序必须为先类别编号文件再特征词文件
        VSMJob.addCacheFile(classIdPath.toUri());
        VSMJob.addCacheFile(eigenvectorPath.toUri());
        VSMJob.setJarByClass(Task2.class);
        VSMJob.setInputFormatClass(KeyValueTextInputFormat.class);
        VSMJob.setOutputFormatClass(TextOutputFormat.class);
        VSMJob.setMapperClass(VSMMapper.class);
        VSMJob.setReducerClass(VSMReducer.class);
        VSMJob.setOutputKeyClass(Text.class);
        VSMJob.setOutputValueClass(Text.class);
        VSMJob.setNumReduceTasks(reduceNum);
        FileInputFormat.addInputPath(VSMJob, new Path(wordNumPath));
        String TFIDFPath = outputPath + "/tf_idf";
        FileOutputFormat.setOutputPath(VSMJob, new Path(TFIDFPath));
        if(!VSMJob.waitForCompletion(true)){
            throw new RuntimeException("TF-IDF/VSM MapReduce Failed");
        }
        return TFIDFPath;
    }
}
