package app.dao;

import java.util.HashMap;
import java.util.Map;

public class CacheData {
    private static Map<String, Integer> pvMap = new HashMap<>();
    private static Map<String, Long> uvMap = new HashMap<>();

    public static void setPvMap(Map<String, Integer> pvMap) {
        CacheData.pvMap = pvMap;
    }

    public static void setUvMap(Map<String, Long> uvMap) {
        CacheData.uvMap = uvMap;
    }

    public static Map<String, Integer> getPvMap() {
        return pvMap;
    }

    public static Map<String, Long> getUvMap() {
        return uvMap;
    }
    public static int getPv(int pv,String indexName){
        Integer cacheValue = pvMap.get(indexName);
        if(cacheValue==null){
            cacheValue=0;
            pvMap.put(indexName,cacheValue);
        }
        if(pv>cacheValue.intValue()){
            pvMap.put(indexName,pv);//将新的值赋值个cacheData
            return pv-cacheValue.intValue();
        }
        return 0;
    }
    public static long getUv(long uv,String indexName){
        Long aLong = uvMap.get(indexName);
        if(aLong==null){
            aLong=0L;
            uvMap.put(indexName,aLong);
        }
        if(uv>aLong.longValue()){
            uvMap.put(indexName,uv);
            return uv-aLong;
        }
        return 0;
    }
}
