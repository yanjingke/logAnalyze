package storm.bean;

public class LogAnalyzeJob {
    private String jobId ;
    private String jobName;
    private int jobType; //1:浏览日志、2:点击日志、3:搜索日志、4:购买日志
    private int bussinessId;//所属业务
    private int status;

    @Override
    public String toString() {
        return "LogAnalyzeJob{" +
                "jobId='" + jobId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", jobType=" + jobType +
                ", bussinessId=" + bussinessId +
                ", status=" + status +
                '}';
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getJobType() {
        return jobType;
    }

    public void setJobType(int jobType) {
        this.jobType = jobType;
    }

    public int getBussinessId() {
        return bussinessId;
    }

    public void setBussinessId(int bussinessId) {
        this.bussinessId = bussinessId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
