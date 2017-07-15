package hadoop.mail.task1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Vevo on 2017/7/14.
 */
public class ParticipleMapper extends Mapper<Object, Text, Text, Text> {
    private Analyzer analyzer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Path[] cacheFiles = context.getLocalCacheFiles();
        FileReader stopWords = new FileReader(cacheFiles[0].toString());
        analyzer = new StandardAnalyzer(stopWords);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String docName = fileSplit.getPath().getName();
        String classLabel = fileSplit.getPath().getParent().getName();

        TokenStream tokenStream = analyzer.tokenStream(docName, value.toString());
        tokenStream = new PorterStemFilter(tokenStream);
        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);

        tokenStream.reset();
        Text mapKey = new Text(docName+"#"+classLabel);
        while(tokenStream.incrementToken()){
            context.write(mapKey, new Text(attr.toString()));
        }
        tokenStream.close();
    }
}
