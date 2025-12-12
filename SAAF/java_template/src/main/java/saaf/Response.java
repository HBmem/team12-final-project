package saaf;

import java.math.BigDecimal;

/**
 * A basic Response object that can be consumed by FaaS Inspector
 * to be used as additional output.
 * 
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Response {
    //
    // User Defined Attributes
    //
    //
    // ADD getters and setters for custom attributes here.
    //

    // Return value
    private String value;

    // Pipeline fields
    private String bucketNamePipeline;
    private String fileNamePipeline;
    private String dbUrlPipeline;
    private BigDecimal loadErrorPipeline;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getBucketNamePipeline() {
        return bucketNamePipeline;
    }

    public void setBucketNamePipeline(String bucketNamePipeline) {
        this.bucketNamePipeline = bucketNamePipeline;
    }

    public String getFileNamePipeline() {
        return fileNamePipeline;
    }

    public void setFileNamePipeline(String fileNamePipeline) {
        this.fileNamePipeline = fileNamePipeline;
    }

    public String getDbUrlPipeline() {
        return dbUrlPipeline;
    }

    public void setDbUrlPipeline(String dbUrlPipeline) {
        this.dbUrlPipeline = dbUrlPipeline;
    }

    public BigDecimal getLoadErrorPipeline() {
        return loadErrorPipeline;
    }

    public void setLoadErrorPipeline(BigDecimal loadErrorPipeline) {
        this.loadErrorPipeline = loadErrorPipeline;
    }

    @Override
    public String toString() {
        return "value=" + this.getValue() +
                ", bucketNamePipeline=" + this.getBucketNamePipeline() +
                ", fileNamePipeline=" + this.getFileNamePipeline() +
                ", dbUrlPipeline=" +  this.getDbUrlPipeline() +
                ", loadErrorPipeline=" + this.getLoadErrorPipeline() +
                super.toString();
    }


}
