package hadoop.mail.task4;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Vevo on 2017/7/21.
 */
public class DistanceUtil {
    public static double getDistance(String train, String test){
        if (test.equals("0")){
            return Double.MAX_VALUE;
        }
        String[] trainVSM = train.split(" ");
        String[] testVSM = test.split(" ");
        String eigenvector;
        double vectorValue;
        double distance = 0.0;
        HashMap<String, Double> VSMMap = new HashMap<>();

        for (String str:trainVSM){
            eigenvector = str.split(":")[0];
            vectorValue = Double.parseDouble(str.split(":")[1]);
            VSMMap.put(eigenvector, vectorValue);
        }
        for (String str:testVSM){
            eigenvector = str.split(":")[0];
            vectorValue = Double.parseDouble(str.split(":")[1]);
            if(VSMMap.containsKey(eigenvector)){
                double trainValue = VSMMap.get(eigenvector);
                VSMMap.put(eigenvector, Math.pow((trainValue-vectorValue),2));
            }else{
                VSMMap.put(eigenvector, Math.pow(vectorValue, 2));
            }
        }

        for(Map.Entry<String, Double> entry:VSMMap.entrySet()){
            distance += entry.getValue();
        }
        //如果觉得没必要可以去掉平方根
        distance = Math.sqrt(distance);

        return distance;
    }
}
