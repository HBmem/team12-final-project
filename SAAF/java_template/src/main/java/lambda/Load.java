package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Properties;

/**
 * AWS Lambda Load Function - Service #2
 * Loads transformed CSV data from S3 into Aurora MySQL database
 * 
 * @author Team12
 */
public class Load implements RequestHandler<Request, HashMap<String, Object>> {
    
    // Database connection parameters loaded from db.properties
    private String url;
    private String username;
    private String password;
    
    /**
     * Load database configuration from db.properties file
     */
    private void loadDatabaseConfig(LambdaLogger logger) throws Exception {
        logger.log("[DEBUG] Starting to load db.properties");
        Properties prop = new Properties();
        InputStream input = Load.class.getClassLoader().getResourceAsStream("db.properties");
        
        if (input == null) {
            logger.log("[ERROR] db.properties file not found!");
            throw new Exception("Unable to find db.properties");
        }
        
        logger.log("[DEBUG] db.properties file found, loading properties");
        prop.load(input);
        
        url = prop.getProperty("url");
        username = prop.getProperty("username");
        password = prop.getProperty("password");
        
        logger.log("[DEBUG] DB Config loaded - URL: " + url + ", Username: " + username);
        
        input.close();
    }
    
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("[DEBUG] ========== LOAD FUNCTION STARTED ==========");
        
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        String bucketName = request.getBucketname();
        String fileName = request.getFilename();
        
        logger.log("[DEBUG] Request received - Bucket: " + bucketName + ", File: " + fileName);
        
        int rowsLoaded = 0;
        int duplicatesSkipped = 0;
        Connection conn = null;
        
        try {
            // Load database configuration
            logger.log("[DEBUG] Step 1: Loading database configuration");
            loadDatabaseConfig(logger);
            logger.log("[DEBUG] Step 1: COMPLETE - Database configuration loaded");
            
            // Download transformed CSV from S3
            logger.log("[DEBUG] Step 2: Starting S3 download");
            inspector.addTimeStamp("s3DownloadStart");
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            logger.log("[DEBUG] S3 client created");
            
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            logger.log("[DEBUG] S3 object retrieved");
            
            InputStream objectData = s3Object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            inspector.addTimeStamp("s3DownloadEnd");
            logger.log("[DEBUG] Step 2: COMPLETE - S3 download finished");
            
            // Connect to Aurora MySQL
            logger.log("[DEBUG] Step 3: Attempting database connection to: " + url);
            logger.log("[DEBUG] This may take 10-30 seconds if database is asleep...");
            inspector.addTimeStamp("dbConnectionStart");
            
            conn = DriverManager.getConnection(url, username, password);
            
            logger.log("[DEBUG] Step 3: COMPLETE - Database connection established!");
            conn.setAutoCommit(false);
            logger.log("[DEBUG] Auto-commit disabled for batch processing");
            inspector.addTimeStamp("dbConnectionEnd");
            
            // Create table if it doesn't exist
            logger.log("[DEBUG] Step 4: Creating table if not exists");
            createTable(conn, logger);
            logger.log("[DEBUG] Step 4: COMPLETE - Table ready");
            
            // Prepare INSERT statement with IGNORE to skip duplicates
            logger.log("[DEBUG] Step 5: Preparing INSERT statement");
            String insertSQL = "INSERT IGNORE INTO sales_data (region, country, item_type, sales_channel, " +
                    "order_priority, order_date, order_id, ship_date, units_sold, unit_price, " +
                    "unit_cost, total_revenue, total_cost, total_profit, order_processing_time, " +
                    "gross_margin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            PreparedStatement pstmt = conn.prepareStatement(insertSQL);
            logger.log("[DEBUG] Step 5: COMPLETE - INSERT statement prepared");
            
            // Skip header line
            String line = reader.readLine();
            logger.log("[DEBUG] Header line skipped: " + line);
            
            // Load data
            logger.log("[DEBUG] Step 6: Starting data load");
            inspector.addTimeStamp("dataLoadStart");
            int batchSize = 0;
            int batchLimit = 1000;
            int totalRows = 0;
            int lineNumber = 1;
            
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                String[] fields = parseCSVLine(line);
                
                if (fields.length < 16) {
                    logger.log("[WARNING] Skipping malformed line " + lineNumber + " (expected 16 fields, got " + 
                        fields.length + ")");
                    continue;
                }
                
                try {
                    // Set parameters for prepared statement
                    pstmt.setString(1, fields[0].trim());
                    pstmt.setString(2, fields[1].trim());
                    pstmt.setString(3, fields[2].trim());
                    pstmt.setString(4, fields[3].trim());
                    pstmt.setString(5, fields[4].trim());
                    pstmt.setString(6, convertDateFormat(fields[5].trim()));
                    pstmt.setInt(7, Integer.parseInt(fields[6].trim()));
                    pstmt.setString(8, convertDateFormat(fields[7].trim()));
                    pstmt.setInt(9, Integer.parseInt(fields[8].trim()));
                    pstmt.setDouble(10, Double.parseDouble(fields[9].trim()));
                    pstmt.setDouble(11, Double.parseDouble(fields[10].trim()));
                    pstmt.setDouble(12, Double.parseDouble(fields[11].trim()));
                    pstmt.setDouble(13, Double.parseDouble(fields[12].trim()));
                    pstmt.setDouble(14, Double.parseDouble(fields[13].trim()));
                    pstmt.setInt(15, Integer.parseInt(fields[14].trim()));
                    pstmt.setDouble(16, Double.parseDouble(fields[15].trim()));
                    
                    pstmt.addBatch();
                    batchSize++;
                    totalRows++;
                    
                    // Execute batch when limit reached
                    if (batchSize >= batchLimit) {
                        logger.log("[DEBUG] Executing batch of " + batchSize + " rows");
                        int[] results = pstmt.executeBatch();
                        conn.commit();
                        
                        for (int result : results) {
                            if (result > 0) rowsLoaded++;
                            else if (result == 0) duplicatesSkipped++;
                        }
                        
                        logger.log("[DEBUG] Batch complete. Total loaded: " + rowsLoaded + 
                            ", Duplicates: " + duplicatesSkipped);
                        batchSize = 0;
                    }
                    
                } catch (Exception e) {
                    logger.log("[ERROR] Error processing line " + lineNumber + ": " + e.getMessage());
                }
            }
            
            // Execute remaining batch
            if (batchSize > 0) {
                logger.log("[DEBUG] Executing final batch of " + batchSize + " rows");
                int[] results = pstmt.executeBatch();
                conn.commit();
                
                for (int result : results) {
                    if (result > 0) rowsLoaded++;
                    else if (result == 0) duplicatesSkipped++;
                }
                
                logger.log("[DEBUG] Final batch complete");
            }
            
            inspector.addTimeStamp("dataLoadEnd");
            logger.log("[DEBUG] Step 6: COMPLETE - Data load finished");
            
            // Cleanup
            pstmt.close();
            reader.close();
            logger.log("[DEBUG] Resources closed");
            
            // Create response
            String successMessage = "Successfully loaded data from " + bucketName + "/" + fileName + 
                ". Rows loaded: " + rowsLoaded + ", Duplicates skipped: " + duplicatesSkipped + 
                ", Total rows processed: " + totalRows;
            
            logger.log("[SUCCESS] " + successMessage);
            
            Response response = new Response();
            response.setValue(successMessage);
            
            // Add load metrics
            inspector.addAttribute("rowsLoaded", rowsLoaded);
            inspector.addAttribute("duplicatesSkipped", duplicatesSkipped);
            inspector.addAttribute("totalRowsProcessed", totalRows);
            inspector.addAttribute("bucketName", bucketName);
            inspector.addAttribute("fileName", fileName);
            inspector.addAttribute("dbUrl", url);
            
            inspector.consumeResponse(response);
            
        } catch (Exception e) {
            logger.log("[CRITICAL ERROR] Exception caught: " + e.getClass().getName());
            logger.log("[CRITICAL ERROR] Message: " + e.getMessage());
            
            // Print full stack trace
            logger.log("[STACK TRACE START]");
            for (StackTraceElement element : e.getStackTrace()) {
                logger.log("  at " + element.toString());
            }
            logger.log("[STACK TRACE END]");
            
            inspector.addAttribute("loadError", e.getMessage());
            
            // Rollback on error
            try {
                if (conn != null) {
                    logger.log("[DEBUG] Attempting rollback");
                    conn.rollback();
                    logger.log("[DEBUG] Rollback successful");
                }
            } catch (Exception rollbackEx) {
                logger.log("[ERROR] Error during rollback: " + rollbackEx.getMessage());
            }
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    logger.log("[DEBUG] Closing database connection");
                    conn.close();
                    logger.log("[DEBUG] Database connection closed");
                }
            } catch (Exception e) {
                logger.log("[ERROR] Error closing connection: " + e.getMessage());
            }
        }
        
        logger.log("[DEBUG] ========== LOAD FUNCTION FINISHED ==========");
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    
    /**
     * Creates the sales_data table if it doesn't exist
     */
    private void createTable(Connection conn, LambdaLogger logger) throws Exception {
        logger.log("[DEBUG] Creating sales_data table if not exists");
        
        String createTableSQL = "CREATE TABLE IF NOT EXISTS sales_data (" +
                "region VARCHAR(100), " +
                "country VARCHAR(100), " +
                "item_type VARCHAR(100), " +
                "sales_channel VARCHAR(50), " +
                "order_priority VARCHAR(50), " +
                "order_date DATE, " +
                "order_id INT PRIMARY KEY, " +
                "ship_date DATE, " +
                "units_sold INT, " +
                "unit_price DECIMAL(10,2), " +
                "unit_cost DECIMAL(10,2), " +
                "total_revenue DECIMAL(12,2), " +
                "total_cost DECIMAL(12,2), " +
                "total_profit DECIMAL(12,2), " +
                "order_processing_time INT, " +
                "gross_margin DECIMAL(5,4), " +
                "INDEX idx_region (region), " +
                "INDEX idx_country (country), " +
                "INDEX idx_item_type (item_type), " +
                "INDEX idx_order_priority (order_priority)" +
                ")";
        
        Statement stmt = conn.createStatement();
        stmt.execute(createTableSQL);
        conn.commit();
        stmt.close();
        
        logger.log("[DEBUG] Table creation/verification complete");
    }
    
    /**
     * Check if table has data and delete it if found
     * Returns the number of rows that were deleted
     */
    private int checkAndClearData(Connection conn, LambdaLogger logger) throws Exception {
        logger.log("[DEBUG] Checking row count in sales_data table");
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as row_count FROM sales_data");
        
        int rowCount = 0;
        if (rs.next()) {
            rowCount = rs.getInt("row_count");
        }
        rs.close();
        
        logger.log("[DEBUG] Found " + rowCount + " existing rows in table");
        
        if (rowCount > 0) {
            logger.log("[DEBUG] Deleting existing data from sales_data table");
            stmt.execute("DELETE FROM sales_data");
            conn.commit();
            logger.log("[DEBUG] Successfully deleted " + rowCount + " rows");
        }
        
        stmt.close();
        return rowCount;
    }
    
    /**
     * Parse CSV line handling commas within quoted fields
     */
    private String[] parseCSVLine(String line) {
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }
    
    /**
     * Convert date from M/d/yyyy format to yyyy-MM-dd for MySQL
     */
    private String convertDateFormat(String dateStr) {
        try {
            SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy");
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
            return outputFormat.format(inputFormat.parse(dateStr));
        } catch (Exception e) {
            return dateStr;
        }
    }
}
