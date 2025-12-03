package lambda;

import com.amazonaws.services.lambda.runtime.Context;
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
    private void loadDatabaseConfig() throws Exception {
        Properties prop = new Properties();
        InputStream input = Load.class.getClassLoader().getResourceAsStream("db.properties");
        
        if (input == null) {
            throw new Exception("Unable to find db.properties");
        }
        
        prop.load(input);
        
        url = prop.getProperty("url");
        username = prop.getProperty("username");
        password = prop.getProperty("password");
        
        input.close();
    }
    
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        String bucketName = request.getBucketname();
        String fileName = request.getFilename();
        
        int rowsLoaded = 0;
        int duplicatesSkipped = 0;
        Connection conn = null;
        
        try {
            // Load database configuration
            loadDatabaseConfig();
            
            // Download transformed CSV from S3
            inspector.addTimeStamp("s3DownloadStart");
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            InputStream objectData = s3Object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            inspector.addTimeStamp("s3DownloadEnd");
            
            // Connect to Aurora MySQL
            inspector.addTimeStamp("dbConnectionStart");
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false); // Use transactions for better performance
            inspector.addTimeStamp("dbConnectionEnd");
            
            // Create table if it doesn't exist
            createTable(conn);
            
            // Prepare INSERT statement with IGNORE to skip duplicates
            String insertSQL = "INSERT IGNORE INTO sales_data (region, country, item_type, sales_channel, " +
                    "order_priority, order_date, order_id, ship_date, units_sold, unit_price, " +
                    "unit_cost, total_revenue, total_cost, total_profit, order_processing_time, " +
                    "gross_margin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            PreparedStatement pstmt = conn.prepareStatement(insertSQL);
            
            // Skip header line
            String line = reader.readLine();
            
            // Load data
            inspector.addTimeStamp("dataLoadStart");
            int batchSize = 0;
            int batchLimit = 1000; // Batch inserts for better performance
            int totalRows = 0;
            
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                String[] fields = parseCSVLine(line);
                
                if (fields.length < 16) {
                    context.getLogger().log("Skipping malformed line (expected 16 fields, got " + 
                        fields.length + "): " + line);
                    continue;
                }
                
                try {
                    // Set parameters for prepared statement
                    pstmt.setString(1, fields[0].trim());  // region
                    pstmt.setString(2, fields[1].trim());  // country
                    pstmt.setString(3, fields[2].trim());  // item_type
                    pstmt.setString(4, fields[3].trim());  // sales_channel
                    pstmt.setString(5, fields[4].trim());  // order_priority
                    
                    // Convert date format from M/d/yyyy to yyyy-MM-dd for MySQL
                    pstmt.setString(6, convertDateFormat(fields[5].trim()));  // order_date
                    pstmt.setInt(7, Integer.parseInt(fields[6].trim()));      // order_id
                    pstmt.setString(8, convertDateFormat(fields[7].trim()));  // ship_date
                    
                    pstmt.setInt(9, Integer.parseInt(fields[8].trim()));      // units_sold
                    pstmt.setDouble(10, Double.parseDouble(fields[9].trim()));  // unit_price
                    pstmt.setDouble(11, Double.parseDouble(fields[10].trim())); // unit_cost
                    pstmt.setDouble(12, Double.parseDouble(fields[11].trim())); // total_revenue
                    pstmt.setDouble(13, Double.parseDouble(fields[12].trim())); // total_cost
                    pstmt.setDouble(14, Double.parseDouble(fields[13].trim())); // total_profit
                    pstmt.setInt(15, Integer.parseInt(fields[14].trim()));   // order_processing_time
                    pstmt.setDouble(16, Double.parseDouble(fields[15].trim())); // gross_margin
                    
                    pstmt.addBatch();
                    batchSize++;
                    totalRows++;
                    
                    // Execute batch when limit reached
                    if (batchSize >= batchLimit) {
                        int[] results = pstmt.executeBatch();
                        conn.commit();
                        for (int result : results) {
                            if (result > 0) rowsLoaded++;
                            else if (result == 0) duplicatesSkipped++;
                        }
                        batchSize = 0;
                    }
                    
                } catch (Exception e) {
                    context.getLogger().log("Error processing row: " + e.getMessage() + " | Line: " + line);
                }
            }
            
            // Execute remaining batch
            if (batchSize > 0) {
                int[] results = pstmt.executeBatch();
                conn.commit();
                for (int result : results) {
                    if (result > 0) rowsLoaded++;
                    else if (result == 0) duplicatesSkipped++;
                }
            }
            
            inspector.addTimeStamp("dataLoadEnd");
            
            // Cleanup
            pstmt.close();
            reader.close();
            
            // Create response
            Response response = new Response();
            response.setValue("Successfully loaded data from " + bucketName + "/" + fileName + 
                ". Rows loaded: " + rowsLoaded + ", Duplicates skipped: " + duplicatesSkipped + 
                ", Total rows processed: " + totalRows);
            
            // Add load metrics
            inspector.addAttribute("rowsLoaded", rowsLoaded);
            inspector.addAttribute("duplicatesSkipped", duplicatesSkipped);
            inspector.addAttribute("totalRowsProcessed", totalRows);
            inspector.addAttribute("bucketName", bucketName);
            inspector.addAttribute("fileName", fileName);
            inspector.addAttribute("dbUrl", url);
            
            inspector.consumeResponse(response);
            
        } catch (Exception e) {
            inspector.addAttribute("loadError", e.getMessage());
            context.getLogger().log("ERROR: " + e.getMessage());
            e.printStackTrace();
            
            // Rollback on error
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (Exception rollbackEx) {
                context.getLogger().log("Error during rollback: " + rollbackEx.getMessage());
            }
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception e) {
                context.getLogger().log("Error closing connection: " + e.getMessage());
            }
        }
        
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    
    /**
     * Creates the sales_data table if it doesn't exist
     */
    private void createTable(Connection conn) throws Exception {
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
    }
    
    /**
     * Parse CSV line handling commas within quoted fields
     */
    private String[] parseCSVLine(String line) {
        // Simple CSV parser that handles quoted fields
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
            // If conversion fails, return original string
            return dateStr;
        }
    }
}