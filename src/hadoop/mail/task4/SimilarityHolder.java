package hadoop.mail.task4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vevo on 2017/7/22.
 * 用于保存k个与测试集相近的训练集数据（类别和距离）
 */
public class SimilarityHolder {
    private ClassSimilarity[] holder;
    private int currentSize;
    private int size;

    class ClassSimilarity {
        String classId;
        double similarity;

        public ClassSimilarity(String classId, double similarity) {
            this.classId = classId;
            this.similarity = similarity;
        }

        public String getClassId() {
            return classId;
        }

        public double getSimilarity() {
            return similarity;
        }

        @Override
        public String toString() {
            return classId + ':' + similarity;
        }
    }

    public SimilarityHolder(int k) {
        this.holder = new ClassSimilarity[k];
        currentSize = 0;
        size = k;
    }

    public int getSize(){
        return size;
    }

    public int getCurrentSize(){
        return currentSize;
    }

    /*
    保证按相似度从小到大排序
     */
    public void insert(String classId, double similarity){
        ClassSimilarity classSimilarity = new ClassSimilarity(classId, similarity);

        if(currentSize <= 0){
            holder[currentSize++] = classSimilarity;
            return;
        }

        int insertPos = 0;
        if(currentSize < size){
            while(insertPos < currentSize){
                if(similarity < holder[insertPos].getSimilarity()){
                    break;
                }
                insertPos ++;
            }
            for(int i = currentSize; i > insertPos; i--){
                holder[i] = holder[i-1];
            }
            holder[insertPos] = classSimilarity;
            currentSize++;
            return;
        }

        if(similarity > holder[insertPos].getSimilarity()){
            while(insertPos < size){
                if(similarity < holder[insertPos].getSimilarity()){
                    break;
                }
                insertPos++;
            }
            insertPos -= 1;

            if(insertPos < 0){
                return;
            }
            for(int i = 0; i < insertPos; i++){
                holder[i] = holder[i+1];
            }
            holder[insertPos] = classSimilarity;
        }
    }

    public String predictClass(){
        HashMap<String, Double> classMap = new HashMap<>();
        for (int i=0; i < currentSize; i++){
            double weight = holder[i].getSimilarity(); //相似度作为权值
            if(classMap.containsKey(holder[i].getClassId())){
                weight += classMap.get(holder[i].getClassId());
            }
            classMap.put(holder[i].getClassId(), weight);
        }

        double maxWeight = Double.MIN_VALUE;
        String maxCountClass = null;
        for (Map.Entry<String, Double> entry:classMap.entrySet()){
            if (entry.getValue() > maxWeight){
                maxWeight = entry.getValue();
                maxCountClass = entry.getKey();
            }
        }
        return maxCountClass;
    }

    @Override
    public String toString() {
        return Arrays.toString(holder);
    }
}
