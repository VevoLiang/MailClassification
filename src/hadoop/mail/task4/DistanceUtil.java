package hadoop.mail.task4;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class DistanceUtil {
    public static double getDistance(String train, String test){
        if (test.equals("0")){
            return Double.MAX_VALUE; //理论上达不到这里，以防万一
        }
        String[] trainVSM = train.split(" ");
        String[] testVSM = test.split(" ");
        LinkedList<String> allVSM = new LinkedList<>(Arrays.asList(trainVSM));
        allVSM.addAll(Arrays.asList(testVSM));
        String vectorId;
        double vectorValue;
        double distance = 0.0;
        HashMap<String, Double> vectorMap = new HashMap<>();

        for(String str:allVSM){
            vectorId = str.split(":")[0];
            vectorValue = Double.parseDouble(str.split(":")[1]);
            if(vectorMap.containsKey(vectorId)){
                double value = vectorMap.get(vectorId);
                vectorMap.put(vectorId, Math.pow((vectorValue-value),2));
            }else{
                vectorMap.put(vectorId, Math.pow(vectorValue, 2));
            }
        }

        for(Map.Entry<String, Double> entry:vectorMap.entrySet()){
            distance += entry.getValue();
        }
        //如果觉得没必要可以去掉平方根
        distance = Math.sqrt(distance);

        return distance;
    }
}
