package hadoop.mail.task3;

import hadoop.mail.task1.Task1;
import hadoop.mail.task2.Task2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by AIbert on 2017/7/19.
 */
public class Task3 {
    public static void main(String[] args) {
        //输入应该为：[stopwords路径]  [task1输出目录]  [task3输入目录(即测试集)]  [task3输出目录]  [reduce数量]
        Path stopWordPath = new Path(args[0]);
        //完整的类别编号文件路径
        Path classIdPath = new Path(args[1] + "/doc_num/part-r-00000");
        Path eigenvectorPath = new Path(args[1] + "/eigenvector/part-r-00000");
        int reduceNum = Integer.parseInt(args[4]);
        //输入路径应该是分词后的文档路径
        String in = args[2];
        String out = args[3];

        try {
            String testPart = Task1.doParticiple(in, out, stopWordPath, reduceNum);
            Task2.getTF(testPart, out, classIdPath, eigenvectorPath, reduceNum);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

 
