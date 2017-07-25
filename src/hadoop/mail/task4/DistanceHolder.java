package hadoop.mail.task4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DistanceHolder {
    private FeatureDistance[] holder;
    private int currentSize;
    private int size;

    class FeatureDistance{
        String classId;
        double distance;

        public FeatureDistance(String classId, double distance) {
            this.classId = classId;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return classId + ":" + distance;
        }
    }

    public DistanceHolder(int size) {
        this.size = size;
        holder = new FeatureDistance[size];
        currentSize = 0;
    }

    /*
    距离越小的越靠后
     */
    public void insert(String classId, double distance){
        FeatureDistance featureDistance = new FeatureDistance(classId, distance);

        if(currentSize == 0){
            holder[currentSize++] = featureDistance;
            return;
        }

        int insertPos = 0;
        if(currentSize < size){
            while(insertPos < currentSize){
                if(distance > holder[insertPos].distance){
                    break;
                }
                insertPos ++;
            }
            for(int i = currentSize; i > insertPos; i--){
                holder[i] = holder[i-1];
            }
            holder[insertPos] = featureDistance;
            currentSize++;
            return;
        }

        while(insertPos < size){
            if(distance > holder[insertPos].distance){
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
        holder[insertPos] = featureDistance;
    }

    public String predictClass(){
        HashMap<String, Integer> classMap = new HashMap<>();
        int count = 0;
        for (int i=0; i < currentSize; i++){
            count = 1;
            if(classMap.containsKey(holder[i].classId)){
                count += classMap.get(holder[i].classId);
            }
            classMap.put(holder[i].classId, count);
        }

        int maxCount = 0;
        String maxCountClass = null;
        for (Map.Entry<String, Integer> entry:classMap.entrySet()){
            if (entry.getValue() > maxCount){
                maxCount = entry.getValue();
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
