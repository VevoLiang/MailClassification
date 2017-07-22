package hadoop.mail.task4;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vevo on 2017/7/22.
 * 用于保存k个与测试集相近的训练集数据（类别和距离）
 */
public class DistanceHolder {
    private ClassDistance[] holder;
    private int currentSize;
    private int size;

    class ClassDistance{
        String classId;
        double distance;

        public ClassDistance(String classId, double distance) {
            this.classId = classId;
            this.distance = distance;
        }

        public String getClassId() {
            return classId;
        }

        public double getDistance() {
            return distance;
        }
    }

    public DistanceHolder(int k) {
        this.holder = new ClassDistance[k];
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
    保证按距离从大到小排序
     */
    public void insert(String classId, double distance){
        ClassDistance classDistance = new ClassDistance(classId, distance);

        if(currentSize <= 0){
            holder[currentSize++] = classDistance;
            return;
        }

        int insertPos = 0;
        if(currentSize < size){
            while(insertPos < currentSize){
                if(distance > holder[insertPos].getDistance()){
                    break;
                }
                insertPos ++;
            }
            for(int i = currentSize; i > insertPos; i--){
                holder[i] = holder[i-1];
            }
            holder[insertPos] = classDistance;
            return;
        }

        if(distance < holder[insertPos].getDistance()){
            while(insertPos < size){
                if(holder[insertPos].getDistance() < distance){
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
            holder[insertPos] = classDistance;
        }
    }

    public String predictClass(){
        HashMap<String, Integer> classMap = new HashMap<>();
        for (int i=0; i < currentSize; i++){
            if(classMap.containsKey(holder[i].getClassId())){
                int currentCount = classMap.get(holder[i].getClassId());
                classMap.put(holder[i].getClassId(), currentCount+1);
            }else {
                classMap.put(holder[i].getClassId(), 1);
            }
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
}
