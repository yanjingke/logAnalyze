package app.callBalk;

import app.bean.BaseRecord;
import app.dao.CacheData;
import redis.clients.jedis.ShardedJedis;
import storm.bean.LogAnalyzeJob;
import storm.dao.LogAnalyzeDao;
import storm.util.DateUtils;
import storm.util.MyShardedJedisPool;

import java.util.*;

public class OneMinuteCallBack implements Runnable {
    @Override
    public void run() {
        Calendar calendar = Calendar.getInstance();
        //24:00分时，将缓存清空
        if (calendar.get(Calendar.MINUTE)==0&&calendar.get(calendar.HOUR)==24){
            CacheData.setPvMap(new HashMap<String, Integer>());
            CacheData.setUvMap(new HashMap<String, Long>());
        }
        String date = DateUtils.getDate();
        //从redis中获取所有的指标最新的值
        List<BaseRecord> baseRecordList = getBaseRecords(date);
        //根据cacheData获取所有的指标的增量值  用最新全量值减去上一个时间段的全量值
        List<BaseRecord> appendDataList = getAppData(baseRecordList);
        //将增量数据保存到mysql中
        new LogAnalyzeDao().saveMinuteAppendRecord(appendDataList);
    }
    private  List<BaseRecord>getAppData(List<BaseRecord> baseRecordList){
        List<BaseRecord>appendDataList=new ArrayList<BaseRecord>();
        for (BaseRecord baseRecord : baseRecordList) {
            int addpv = CacheData.getPv(baseRecord.getPv(), baseRecord.getIndexName());
            long adduv =CacheData.getUv(baseRecord.getUv(),baseRecord.getIndexName());
            appendDataList.add(new BaseRecord(baseRecord.getIndexName(),addpv,adduv,baseRecord.getProcessTime()));

        }
        return appendDataList;
    }
    private List<BaseRecord>getBaseRecords(String date){
        List<LogAnalyzeJob> logAnalyzeJobs = new LogAnalyzeDao().loadJobList();
        ShardedJedis jedis = MyShardedJedisPool.getShardedJedisPool().getResource();
        List<BaseRecord>baseRecords=new ArrayList<BaseRecord>();
        for(LogAnalyzeJob logAnalyzeJob:logAnalyzeJobs){
            String pvKey = "log:" + logAnalyzeJob.getJobName() + ":pv:" + date;
            String uvKey = "log:" + logAnalyzeJob.getJobName() + ":uv:" + date;
            String pv = jedis.get(pvKey);
            Long  uv= jedis.scard(uvKey);
            BaseRecord baseRecord =new BaseRecord(logAnalyzeJob.getJobName(), Integer.parseInt(pv.trim()), uv, new Date());
            baseRecords.add(baseRecord);
        }
        return baseRecords;
    }
}
