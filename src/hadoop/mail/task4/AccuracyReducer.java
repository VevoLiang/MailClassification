package hadoop.mail.task4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AccuracyReducer extends Reducer<Text, IntWritable, Text, Text> {
    private double trueCount = 0;
    private double falseCount = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int value = 0;
        for (IntWritable i:values){
            value += i.get();
        }
        if(key.toString().equals("true")){
            trueCount += value;
        }else{
            falseCount += value;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Correct rate:"), new Text(trueCount/(trueCount+falseCount) * 100 + "%"));
        super.cleanup(context);
    }
}
