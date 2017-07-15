package hadoop.mail.task1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Vevo on 2017/7/14.
 */
public class WordInClassMapper extends Mapper<Text, Text, Text, IntWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String classLabel = key.toString().split("#")[1];
        String[] words = value.toString().split(",");
        HashSet<String> uniqueWords = new HashSet<String>();
        Collections.addAll(uniqueWords, words);
        for(String word:uniqueWords){
            context.write(new Text(word + "#" + classLabel), new IntWritable(1));
        }
    }
}
