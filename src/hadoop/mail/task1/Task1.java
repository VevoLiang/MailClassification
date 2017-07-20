package hadoop.mail.task1;

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
 * Created by Vevo on 2017/7/14.
 */
public class Task1 {
    public static void main(String[] args) {
        //每个类提取100个特征词
        int kEigenvector = 100;
        Path stopWordFile = new Path(args[0]);
        int reduceNum = Integer.parseInt(args[3]);
        String in = args[1];
        String out = args[2];

        try {
            String partPath = doParticiple(in, out, stopWordFile, reduceNum);
            String wordInClassPath = getWordInClass(partPath, out, reduceNum);
            String classDocPath = getDocNum(partPath, out);
            doExtraction(classDocPath, wordInClassPath, out, kEigenvector);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static String doParticiple(String inputPath, String outputPath, Path stopWordFile, int reduceNum) throws IOException, ClassNotFoundException, InterruptedException {
        //先进行分词，将类别文档转换成行存放在数据文件中（文档数量多且文件小时hadoop效率低）
        Configuration conf = new Configuration();
        Job participleJob = Job.getInstance(conf,"Participle");
        participleJob.addCacheFile(stopWordFile.toUri());
        participleJob.setJarByClass(Task1.class);
        participleJob.setInputFormatClass(TextInputFormat.class);
        participleJob.setOutputFormatClass(TextOutputFormat.class);
        participleJob.setMapperClass(ParticipleMapper.class);
        //Map输出和Reduce类型一致可省略
        //participleJob.setMapOutputKeyClass(Text.class);
        //participleJob.setMapOutputValueClass(Text.class);
        participleJob.setReducerClass(ParticipleReducer.class);
        participleJob.setOutputKeyClass(Text.class);
        participleJob.setOutputValueClass(Text.class);
        participleJob.setNumReduceTasks(reduceNum);
        Path partPath = new Path(outputPath + "/part");
        FileInputFormat.addInputPath(participleJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(participleJob, partPath);
        if(!participleJob.waitForCompletion(true)){
            throw new RuntimeException("Participle MapReduce FAILED");
        }
        return partPath.toString();
    }

    private static String getWordInClass(String inputPath, String outputPath, int reduceNum) throws IOException, ClassNotFoundException, InterruptedException {
        //为了减少内存消耗这里把计算单词在各类别出现文档数分为两个MapReduce完成（可合并）
        Configuration conf1 = new Configuration();
        Job wordInClassJob = Job.getInstance(conf1, "WordInClass");
        wordInClassJob.setJarByClass(Task1.class);
        wordInClassJob.setInputFormatClass(KeyValueTextInputFormat.class);
        wordInClassJob.setOutputFormatClass(TextOutputFormat.class);
        wordInClassJob.setMapperClass(WordInClassMapper.class);
        wordInClassJob.setReducerClass(WordInClassReducer.class);
        wordInClassJob.setOutputKeyClass(Text.class);
        wordInClassJob.setOutputValueClass(IntWritable.class);
        wordInClassJob.setNumReduceTasks(reduceNum);
        Path wordClassPath = new Path(outputPath + "/word_in_class");
        FileInputFormat.addInputPath(wordInClassJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(wordInClassJob, wordClassPath);
        wordInClassJob.waitForCompletion(true);
        //第二部分
        Configuration conf2 = new Configuration();
        Job wordInClassJob2 = Job.getInstance(conf2, "WordClassVector");
        wordInClassJob2.setJarByClass(Task1.class);
        wordInClassJob2.setInputFormatClass(KeyValueTextInputFormat.class);
        wordInClassJob2.setOutputFormatClass(TextOutputFormat.class);
        wordInClassJob2.setMapperClass(WordInClass2Mapper.class);
        wordInClassJob2.setReducerClass(WordInClass2Reducer.class);
        wordInClassJob2.setOutputKeyClass(Text.class);
        wordInClassJob2.setOutputValueClass(Text.class);
        wordInClassJob2.setNumReduceTasks(reduceNum);
        Path wordClass2Path = new Path(outputPath + "/word_class_vector");
        FileInputFormat.addInputPath(wordInClassJob2, wordClassPath);
        FileOutputFormat.setOutputPath(wordInClassJob2, wordClass2Path);
        if(!wordInClassJob2.waitForCompletion(true)){
            throw new RuntimeException("WordInClass MapReduce FAILED");
        }
        return wordClass2Path.toString();
    }

    private static String getDocNum(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        //不设置reducer数量默认为1，方便计算总文档数
        Configuration conf = new Configuration();
        Job docNumJob = Job.getInstance(conf, "ClassDocNum");
        docNumJob.setJarByClass(Task1.class);
        docNumJob.setInputFormatClass(KeyValueTextInputFormat.class);
        docNumJob.setOutputFormatClass(TextOutputFormat.class);
        docNumJob.setMapperClass(DocNumMapper.class);
        docNumJob.setReducerClass(DocNumReducer.class);
        docNumJob.setOutputKeyClass(Text.class);
        docNumJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(docNumJob, new Path(inputPath));
        Path classDocPath = new Path(outputPath + "/doc_num");
        //classDocPath.getFileSystem(conf).delete(classDocPath, true);
        FileOutputFormat.setOutputPath(docNumJob, classDocPath);
        if(!docNumJob.waitForCompletion(true)){
            throw new RuntimeException("ClassDocNum MapReduce FAILED");
        }
        return classDocPath.toString();
    }

    private static String doExtraction(String cacheFilePath, String inputPath, String outputPath, int kEigenvector) throws InterruptedException, IOException, ClassNotFoundException {
        //由于Reducer默认为1，所以可以直接合并所有特征词并编号
        Configuration conf = new Configuration();
        //类别提取特征数量传入到全局参数,，要在创建Job之前
        conf.setInt("kEigenvector", kEigenvector);
        Job extractJob = Job.getInstance(conf, "Extraction");
        extractJob.setJarByClass(Task1.class);
        //将类别文档数放入到全局文件
        extractJob.addCacheFile(URI.create(cacheFilePath.toString() + "/part-r-00000"));
        extractJob.setInputFormatClass(KeyValueTextInputFormat.class);
        extractJob.setOutputFormatClass(TextOutputFormat.class);
        extractJob.setMapperClass(ExtractMapper.class);
        extractJob.setReducerClass(ExtractReducer.class);
        extractJob.setMapOutputKeyClass(Text.class);
        extractJob.setMapOutputValueClass(Text.class);
        extractJob.setOutputKeyClass(Text.class);
        extractJob.setOutputValueClass(Text.class);
        Path extractionPath = new Path(outputPath + "/eigenvector");
        FileInputFormat.addInputPath(extractJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(extractJob, extractionPath);
        if(!extractJob.waitForCompletion(true)){
            throw new RuntimeException("Extraction MapReduce FAILED");
        }
        return extractionPath.toString();
    }
}
