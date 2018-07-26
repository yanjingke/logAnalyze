package storm.util;

import com.google.gson.Gson;
import redis.clients.jedis.ShardedJedis;
import storm.bean.LogAnalyzeJob;
import storm.bean.LogAnalyzeJobDetail;
import storm.bean.LogMessage;
import storm.bean.LogTypeConstant;
import storm.dao.LogAnalyzeDao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogAnalyzeHandler {
    //定时加载配置文件的标识
    private static boolean reloaded = false;
    //用来保存job信息，key为jobType，value为该类别下所有的任务。
    private static Map<String, List<LogAnalyzeJob>> jobMap;
    //用来保存job的判断条件，key为jobId,value为list，list中封装了很多判断条件。
    private static Map<String, List<LogAnalyzeJobDetail>> jobDetail;
    static ShardedJedis jedis = MyShardedJedisPool.getShardedJedisPool().getResource();
    public static LogMessage parser(String line){
        LogMessage logMessage = new Gson().fromJson(line, LogMessage.class);
        return logMessage;
    }
    public static boolean isValidType(int jobType){
        if(jobType==LogTypeConstant.VIEW||jobType==LogTypeConstant.CLICK||jobType==LogTypeConstant.SEARCH||jobType==LogTypeConstant.BUY){
            return  true;
        }
        return false;
    }
    private static Map<String,List<LogAnalyzeJob>>loadJobMap(){
        Map<String,List<LogAnalyzeJob>>map=new HashMap<String,List<LogAnalyzeJob>>();
        List<LogAnalyzeJob> logAnalyzeJobs = new LogAnalyzeDao().loadJobList();
        for (LogAnalyzeJob logAnalyzeJob:logAnalyzeJobs){
            int jobType = logAnalyzeJob.getJobType();
            if (isValidType(jobType)){
                List<LogAnalyzeJob> jobList = map.get(jobType+"");
                if(jobList==null||jobList.size()==0){
                    jobList = new ArrayList<>();
                    map.put(jobType+"",jobList);
                }
                jobList.add(logAnalyzeJob);
            }
        }
        System.out.println("job:  " + map);
        return map;
    }
    private static  Map<String,List<LogAnalyzeJobDetail>>loadJobDetailMap(){
        Map<String,List<LogAnalyzeJobDetail>>map=new HashMap<String,List<LogAnalyzeJobDetail>>();
        List<LogAnalyzeJobDetail> logAnalyzeJobDetails = new LogAnalyzeDao().loadJobDetailList();
        for(LogAnalyzeJobDetail logAnalyzeJobDetail :logAnalyzeJobDetails){
            int jobId=logAnalyzeJobDetail.getJobId();
            List<LogAnalyzeJobDetail> logJobDetails = map.get(jobId + "");
            if (logJobDetails==null||logJobDetails.size()==0){
                logJobDetails=new ArrayList<>();
                map.put(jobId + "",logJobDetails);
            };
            logJobDetails.add( logAnalyzeJobDetail);
        }
        System.out.println("job:  " + map);
        return map;
    }
    private synchronized static void loadDataModel() {
        if (jobMap == null) {
            jobMap = loadJobMap();
        }
        if (jobDetail == null) {
            jobDetail = loadJobDetailMap();
        }
    }

    /**
     * 配置scheduleLoad重新加载底层数据模型。
     */
    public static synchronized void reloadDataModel() {
        if (reloaded) {
            jobMap = loadJobMap();
            jobDetail = loadJobDetailMap();
            reloaded = false;
        }
    }
       /* 配合reloadDataModel模块一起使用。
            * 主要实现原理如下：
            * 1，获取分钟的数据值，当分钟数据是10的倍数，就会触发reloadDataModel方法，简称reload时间。
            * 2，reloadDataModel方式是线程安全的，在当前worker中只有一个现成能够操作。
            * 3，为了保证当前线程操作完毕之后，其他线程不再重复操作，设置了一个标识符reloaded。
            * 在非reload时间段时，reloaded一直被置为true；
            * 在reload时间段时，第一个线程进入reloadDataModel后，加载完毕之后会将reloaded置为false。
            */
    public static void scheduleLoad() {
        String date = DateUtils.getDateTime();
        int now = Integer.parseInt(date.split(":")[1]);
        if (now % 10 == 0) {//每10分钟加载一次
            reloadDataModel();
        } else {
            reloaded = true;
        }
    }
    /**
     * pv 在redis中是string，key为：log:{jobId}:pv:{20151116}，value=pv数量。
     * uv 使用java-bloomFilter计算，https://github.com/maoxiangyi/Java-BloomFilter
     *
     * @param logMessage
     */
    public static void process( LogMessage logMessage){

        if (jobMap == null || jobDetail == null) {
            loadDataModel();
        }

        List<LogAnalyzeJob> logAnalyzeJobs = jobMap.get(logMessage.getType()+"");
        for(LogAnalyzeJob logAnalyzeJob:logAnalyzeJobs){
            boolean isMatch = false; //是否匹配
            List<LogAnalyzeJobDetail> logAnalyzeJobDetails = jobDetail.get(logAnalyzeJob.getJobId());
            for(LogAnalyzeJobDetail logAnalyzeJobDetail:logAnalyzeJobDetails){
                //jobDetail,指定和kakfa输入过来的数据中的 requesturl比较
                // 获取kafka输入过来的数据的requesturl的值
                String fieldValueInLog=logMessage.getCompareFieldValue(logAnalyzeJobDetail.getField());
                if(logAnalyzeJobDetail.getCompare()==1 && fieldValueInLog.contains(logAnalyzeJobDetail.getValue())){
                    isMatch = true;
                }
                 else if(logAnalyzeJobDetail.getCompare()==2&&fieldValueInLog.equals(logAnalyzeJobDetail.getValue())){
                    isMatch = true;
                }else{
                     isMatch = false;
                }
                if (!isMatch) {
                    break;
                }

            }
            if (isMatch){
                String pvKey="log:"+logAnalyzeJob.getJobName()+":pv:"+DateUtils.getDate();
                String uvKey = "log:" + logAnalyzeJob.getJobName()+":uv:"+DateUtils.getDate();

                //给pv+1
                jedis.incr(pvKey);
                //System.out.println( jedis.get(pvKey));
                //设置uv，uv需要去重，使用set
                jedis.sadd(uvKey, logMessage.getUserName());

            }
        }
    }
}

