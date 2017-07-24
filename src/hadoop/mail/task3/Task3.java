package hadoop.mail.task3;

import hadoop.mail.task1.Task1;
import hadoop.mail.task2.Task2;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by AIbert on 2017/7/19.
 */
public class Task3 {
    //输入应该为：[停词表路径]  [task1输出目录]  [task3输入目录(即测试集)]  [task3输出目录]  [reduce数量]
    public static void main(String[] args) {
        Path stopWordPath = new Path(args[0]);
        //完整的类别编号文件路径
        Path classIdPath = new Path(args[1] + "/doc_num/part-r-00000");
        Path eigenvectorPath = new Path(args[1] + "/eigenvector/part-r-00000");
        String in = args[2];
        String out = args[3];
        int reduceNum = Integer.parseInt(args[4]);

        try {
            String testPartPath = Task1.doParticiple(in, out, stopWordPath, reduceNum);
            Task2.getTFIDF(testPartPath, out, classIdPath, eigenvectorPath, reduceNum);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

 
