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

    public void insert(String classId, double similarity){
        ClassSimilarity classSimilarity = new ClassSimilarity(classId, similarity);

        if(currentSize < size){
            holder[currentSize++] = classSimilarity;
            return;
        }

        int insertPos = -1;
        double min = similarity;
        for(int i=0; i<size; i++){
            if(min > holder[i].similarity){
                insertPos = i;
                min = holder[i].similarity;
            }
        }
        if(insertPos >= 0){
            holder[insertPos] = classSimilarity;
        }
    }

    public String predictClass(){
        HashMap<String, Double> classMap = new HashMap<>();
        for (int i=0; i < currentSize; i++){
            double weight = holder[i].similarity; //相似度作为权值
            if(classMap.containsKey(holder[i].classId)){
                weight += classMap.get(holder[i].classId);
            }
            classMap.put(holder[i].classId, weight);
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
