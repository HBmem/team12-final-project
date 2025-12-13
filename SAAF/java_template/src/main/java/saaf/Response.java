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
    private String bucketName;
    private String fileName;
    private String dbUrl;
    private BigDecimal loadError;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public BigDecimal getLoadError() {
        return loadError;
    }

    public void setLoadError(BigDecimal loadError) {
        this.loadError = loadError;
    }

    @Override
    public String toString() {
        return "value=" + this.getValue() +
                ", bucketName=" + this.getBucketName() +
                ", fileName=" + this.getFileName() +
                ", dbUrl=" +  this.getDbUrl() +
                ", loadError=" + this.getLoadError() +
                super.toString();
    }


}
