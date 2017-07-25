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
        String[] allVSM = (train + " " + test).split(" ");
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

    public static double getCosDistance(String train, String test){
        if (test.equals("0")){
            return Double.MAX_VALUE; //理论上达不到这里，以防万一
        }
        String[] trainVSM = train.split(" ");
        String[] testVSM = test.split(" ");
        HashMap<String, Double> trainMap = new HashMap<>();
        HashMap<String, Double> testMap = new HashMap<>();
        for(String str:trainVSM){
            trainMap.put(str.split(":")[0], Double.parseDouble(str.split(":")[1]));
        }
        for(String str:testVSM){
            testMap.put(str.split(":")[0], Double.parseDouble(str.split(":")[1]));
        }

        double molecular = 0.0;
        double denominator = 0.0;
        double d1 = 0.0;
        for(Map.Entry<String, Double> entry:trainMap.entrySet()){
            d1 += Math.pow(entry.getValue(),2);
            if(testMap.containsKey(entry.getKey())){
                molecular += testMap.get(entry.getKey()) * entry.getValue();
            }
        }
        d1 = Math.sqrt(d1);
        double d2 = 0.0;
        for(Map.Entry<String, Double> entry:testMap.entrySet()){
            d2 += Math.pow(entry.getValue(), 2);
        }
        d2 = Math.sqrt(d2);
        denominator = d1 * d2;
        return molecular/denominator;
    }
}
