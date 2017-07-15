package hadoop.mail.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Vevo on 2017/7/14.
 */
public class ChildTask1 {
    private static Logger logger = Logger.getLogger(ChildTask1.class);

    public static void main(String[] args) {
        Path stopWordFile = new Path(args[0]);
        Path in = new Path(args[1]);
        Path partPath = new Path(args[2] + "/part");
        Path wordClassPath = new Path(args[2] + "/word_in_class");
        Path vectorPath = new Path(args[2] + "/word_class_vector");
        Path classDocPath = new Path(args[2] + "/doc_num");
        Path extractedPath = new Path(args[2] + "/extraction");
        Path eigenvectorPath = new Path(args[2] + "/eigenvector");
        int reduceNum = Integer.parseInt(args[3]);
        int kEigenvector = 100; //每个类提取100个特征词

        try {
////            先进行分词，将类别文档转换成行存放在数据文件中（文档数量多且文件小时hadoop效率低）
//            Configuration conf1 = new Configuration();
//            Job participleJob = Job.getInstance(conf1,"Participle");
//            participleJob.addCacheFile(stopWordFile.toUri());
//            participleJob.setJarByClass(ChildTask1.class);
//            participleJob.setInputFormatClass(TextInputFormat.class);
//            participleJob.setOutputFormatClass(TextOutputFormat.class);
//            participleJob.setMapperClass(ParticipleMapper.class);
////            Map输出和Reduce类型一致可省略
////            participleJob.setMapOutputKeyClass(Text.class);
////            participleJob.setMapOutputValueClass(Text.class);
//            participleJob.setReducerClass(ParticipleReducer.class);
//            participleJob.setOutputKeyClass(Text.class);
//            participleJob.setOutputValueClass(Text.class);
//            participleJob.setNumReduceTasks(reduceNum);
//            FileInputFormat.addInputPath(participleJob, in);
//            FileOutputFormat.setOutputPath(participleJob, partPath);
//            participleJob.waitForCompletion(true);
//
////            为了减少内存消耗这里把计算单词在各类别出现文档数的向量分为两个MapReduce完成（可合并）
//            Configuration conf2 = new Configuration();
//            Job vectorJob1 = Job.getInstance(conf2, "WordInClass");
//            vectorJob1.setJarByClass(ChildTask1.class);
//            vectorJob1.setInputFormatClass(KeyValueTextInputFormat.class);
//            vectorJob1.setOutputFormatClass(TextOutputFormat.class);
//            vectorJob1.setMapperClass(WordInClassMapper.class);
//            vectorJob1.setReducerClass(WordInClassReducer.class);
//            vectorJob1.setOutputKeyClass(Text.class);
//            vectorJob1.setOutputValueClass(IntWritable.class);
//            vectorJob1.setNumReduceTasks(reduceNum);
//            FileInputFormat.addInputPath(vectorJob1, partPath);
//            FileOutputFormat.setOutputPath(vectorJob1, wordClassPath);
//            vectorJob1.waitForCompletion(true);
////            第二部分
//            Configuration conf3 = new Configuration();
//            Job vectorJob2 = Job.getInstance(conf3, "WordClassVector");
//            vectorJob2.setJarByClass(ChildTask1.class);
//            vectorJob2.setInputFormatClass(KeyValueTextInputFormat.class);
//            vectorJob2.setOutputFormatClass(TextOutputFormat.class);
//            vectorJob2.setMapperClass(VectorMapper.class);
//            vectorJob2.setReducerClass(VectorReducer.class);
//            vectorJob2.setOutputKeyClass(Text.class);
//            vectorJob2.setOutputValueClass(Text.class);
//            vectorJob2.setNumReduceTasks(reduceNum);
//            FileInputFormat.addInputPath(vectorJob2, wordClassPath);
//            FileOutputFormat.setOutputPath(vectorJob2, vectorPath);
//            vectorJob2.waitForCompletion(true);

////            不设置reducer数量默认为1，方便计算总文档数
//            Configuration conf4 = new Configuration();
//            Job docNumJob = Job.getInstance(conf4, "ClassDocNum");
//            docNumJob.setJarByClass(ChildTask1.class);
//            docNumJob.setInputFormatClass(KeyValueTextInputFormat.class);
//            docNumJob.setOutputFormatClass(TextOutputFormat.class);
//            docNumJob.setMapperClass(DocNumMapper.class);
//            docNumJob.setReducerClass(DocNumReducer.class);
//            docNumJob.setOutputKeyClass(Text.class);
//            docNumJob.setOutputValueClass(Text.class);
//            FileInputFormat.addInputPath(docNumJob, partPath);
//            classDocPath.getFileSystem(conf4).delete(classDocPath, true);
//            FileOutputFormat.setOutputPath(docNumJob, classDocPath);
//            docNumJob.waitForCompletion(true);

//            logger.warn(URI.create(classDocPath.toString() + "/part-r-00000").toString());
//            logger.warn(new Path(classDocPath.toString() + "/part-r-00000").toUri().toString());

//            由于Reducer默认为1，所以可以直接合并所有特征词并编号
            Configuration conf5 = new Configuration();
//            类别提取特征数量传入到全局参数,，要在创建Job之前
            conf5.setInt("kEigenvector", kEigenvector);
            Job extractJob = Job.getInstance(conf5, "Extract");
            extractJob.setJarByClass(ChildTask1.class);
//            将类别文档数放入到全局文件
            extractJob.addCacheFile(URI.create(classDocPath.toString() + "/part-r-00000"));
            extractJob.setInputFormatClass(KeyValueTextInputFormat.class);
            extractJob.setOutputFormatClass(TextOutputFormat.class);
            extractJob.setMapperClass(ExtractMapper.class);
            extractJob.setReducerClass(ExtractReducer.class);
            extractJob.setMapOutputKeyClass(Text.class);
            extractJob.setMapOutputValueClass(Text.class);
            extractJob.setOutputKeyClass(Text.class);
            extractJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(extractJob, vectorPath);
            FileOutputFormat.setOutputPath(extractJob, extractedPath);
            extractJob.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
