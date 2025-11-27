package lambda;

/**
 * Response class for passing data between Lambda functions
 * 
 * @author Team12
 */
public class Response {
    private String bucketName;
    private String fileName;
    private int rowsProcessed;
    private String status;
    private String message;
    private String dbEndpoint;
    private String dbName;
    
    public Response() {
    }
    
    public Response(String bucketName, String fileName) {
        this.bucketName = bucketName;
        this.fileName = fileName;
    }
    
    // Getters and Setters
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
    
    public int getRowsProcessed() {
        return rowsProcessed;
    }
    
    public void setRowsProcessed(int rowsProcessed) {
        this.rowsProcessed = rowsProcessed;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public String getDbEndpoint() {
        return dbEndpoint;
    }
    
    public void setDbEndpoint(String dbEndpoint) {
        this.dbEndpoint = dbEndpoint;
    }
    
    public String getDbName() {
        return dbName;
    }
    
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}