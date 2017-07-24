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
    //输入应该为：[task1输出目录] [task2输出目录] [reduce数量]
    public static void main(String[] args) {
        //完整的类别编号文件路径
        Path classIdPath = new Path(args[0] + "/doc_num/part-r-00000");
        Path eigenvectorPath = new Path(args[0] + "/eigenvector/part-r-00000");
        //分词后的文档路径
        String in = args[0] + "/part";
        String out = args[1];
        int reduceNum = Integer.parseInt(args[2]);

        try {
            getTFIDF(in, out, classIdPath, eigenvectorPath, reduceNum);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static String getTFIDF(String inputPath, String outputPath, Path classIdPath, Path eigenvectorPath, int reduceNum) throws IOException, ClassNotFoundException, InterruptedException {
        //先计算词频次数
        Configuration conf1 = new Configuration();
        Job wordNumJob = Job.getInstance(conf1, "WordNumInDoc");
        wordNumJob.addCacheFile(classIdPath.toUri()); //类别编号文件
        wordNumJob.setJarByClass(Task2.class);
        wordNumJob.setInputFormatClass(KeyValueTextInputFormat.class);
        wordNumJob.setOutputFormatClass(TextOutputFormat.class);
        wordNumJob.setMapperClass(WordNumMapper.class);
        wordNumJob.setReducerClass(WordNumReducer.class);
        wordNumJob.setOutputKeyClass(Text.class);
        wordNumJob.setOutputValueClass(IntWritable.class);
        wordNumJob.setNumReduceTasks(reduceNum);
        FileInputFormat.addInputPath(wordNumJob, new Path(inputPath));
        String wordNumPath = outputPath + "/word_num_in_doc";
        FileOutputFormat.setOutputPath(wordNumJob, new Path(wordNumPath));
        wordNumJob.waitForCompletion(true);

        //利用Task1保存的IDF信息计算TF-IDF
        Configuration conf2 = new Configuration();
        Job VSMJob = Job.getInstance(conf2, "TF-IDF/VSM");
        VSMJob.addCacheFile(eigenvectorPath.toUri()); //特征词文件
        VSMJob.setJarByClass(Task2.class);
        VSMJob.setInputFormatClass(KeyValueTextInputFormat.class);
        VSMJob.setOutputFormatClass(TextOutputFormat.class);
        VSMJob.setMapperClass(VSMMapper.class);
        VSMJob.setReducerClass(VSMReducer.class);
        VSMJob.setOutputKeyClass(Text.class);
        VSMJob.setOutputValueClass(Text.class);
        //后面用到该训练数据，因而应该只生成一个文件
        //VSMJob.setNumReduceTasks(reduceNum);
        FileInputFormat.addInputPath(VSMJob, new Path(wordNumPath));
        String TFIDFPath = outputPath + "/tf_idf";
        FileOutputFormat.setOutputPath(VSMJob, new Path(TFIDFPath));
        if(!VSMJob.waitForCompletion(true)){
            throw new RuntimeException("TF-IDF/VSM MapReduce Failed");
        }
        return TFIDFPath;
    }
}
