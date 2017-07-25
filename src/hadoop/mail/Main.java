package hadoop.mail;

import hadoop.mail.task1.Task1;
import hadoop.mail.task2.Task2;
import hadoop.mail.task3.Task3;
import hadoop.mail.task4.Task4;

/**
 * Created by Vevo on 2017/7/23.
 */
public class Main {
    //输入参数为：[停词表文件路径]  [训练集路径]  [测试集路径]  [输出路径]  [reduceNum]  [类别特征数]  [KNN的K值](可选默认为1)
    public static void main(String[] args) {
        String[] taskInput = new String[5];
        taskInput[0] = args[0];
        taskInput[1] = args[1];
        taskInput[2] = args[3] + "/step1";
        taskInput[3] = args[4];
        taskInput[4] = args[5];
        Task1.main(taskInput);

        taskInput = new String[3];
        taskInput[0] = args[3] + "/step1";
        taskInput[1] = args[3] + "/step2";
        taskInput[2] = args[4];
        Task2.main(taskInput);

        taskInput = new String[5];
        taskInput[0] = args[0];
        taskInput[1] = args[3] + "/step1";
        taskInput[2] = args[2];
        taskInput[3] = args[3] + "/step3";
        taskInput[4] = args[4];
        Task3.main(taskInput);

        taskInput = new String[4];
        taskInput[0] = args[3] + "/step2";
        taskInput[1] = args[3] + "/step3";
        taskInput[2] = args[3] + "/step4";
        if(args.length == 7){
            taskInput[3] = args[6];
        }else{
            taskInput[3] = "1";
        }
        Task4.main(taskInput);
    }
}
