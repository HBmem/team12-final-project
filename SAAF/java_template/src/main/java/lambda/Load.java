package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;

/**
 * AWS Lambda Load Function - Service #2
 * Loads transformed CSV data from S3 into Aurora MySQL database
 * 
 * @author Team12
 */
public class Load implements RequestHandler<Request, HashMap<String, Object>> {
    
    // Aurora MySQL connection parameters - set these as environment variables in Lambda
    private static final String DB_ENDPOINT = System.getenv("DB_ENDPOINT");
    private static final String DB_NAME = System.getenv("DB_NAME");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASSWORD = System.getenv("DB_PASSWORD");
    
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        String bucketName = request.getBucketName();
        String fileName = request.getFilename();
        
        int rowsLoaded = 0;
        int duplicatesSkipped = 0;
        Connection conn = null;
        
        try {
            // Download transformed CSV from S3
            inspector.addTimeStamp("s3DownloadStart");
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            InputStream objectData = s3Object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            inspector.addTimeStamp("s3DownloadEnd");
            
            // Connect to Aurora MySQL
            inspector.addTimeStamp("dbConnectionStart");
            String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s", DB_ENDPOINT, DB_NAME);
            conn = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD);
            inspector.addTimeStamp("dbConnectionEnd");
            
            // Create table if it doesn't exist
            createTable(conn);
            
            // Prepare INSERT statement
            String insertSQL = "INSERT INTO sales_data (region, country, item_type, sales_channel, " +
                    "order_priority, order_date, order_id, ship_date, units_sold, unit_price, " +
                    "unit_cost, total_revenue, total_cost, total_profit, order_processing_time, " +
                    "gross_margin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE order_id=order_id";
            
            PreparedStatement pstmt = conn.prepareStatement(insertSQL);
            
            // Skip header line
            String line = reader.readLine();
            
            // Load data
            inspector.addTimeStamp("dataLoadStart");
            int batchSize = 0;
            int batchLimit = 1000; // Batch inserts for better performance
            
            while ((line = reader.readLine()) != null) {
                String[] fields = parseCSVLine(line);
                
                if (fields.length < 16) {
                    context.getLogger().log("Skipping malformed line: " + line);
                    continue;
                }
                
                try {
                    // Set parameters for prepared statement
                    pstmt.setString(1, fields[0]);  // region
                    pstmt.setString(2, fields[1]);  // country
                    pstmt.setString(3, fields[2]);  // item_type
                    pstmt.setString(4, fields[3]);  // sales_channel
                    pstmt.setString(5, fields[4]);  // order_priority
                    pstmt.setString(6, fields[5]);  // order_date
                    pstmt.setInt(7, Integer.parseInt(fields[6]));     // order_id
                    pstmt.setString(8, fields[7]);  // ship_date
                    pstmt.setInt(9, Integer.parseInt(fields[8]));     // units_sold
                    pstmt.setDouble(10, Double.parseDouble(fields[9]));  // unit_price
                    pstmt.setDouble(11, Double.parseDouble(fields[10])); // unit_cost
                    pstmt.setDouble(12, Double.parseDouble(fields[11])); // total_revenue
                    pstmt.setDouble(13, Double.parseDouble(fields[12])); // total_cost
                    pstmt.setDouble(14, Double.parseDouble(fields[13])); // total_profit
                    pstmt.setInt(15, Integer.parseInt(fields[14]));   // order_processing_time
                    pstmt.setDouble(16, Double.parseDouble(fields[15])); // gross_margin
                    
                    pstmt.addBatch();
                    batchSize++;
                    
                    // Execute batch when limit reached
                    if (batchSize >= batchLimit) {
                        int[] results = pstmt.executeBatch();
                        for (int result : results) {
                            if (result > 0) rowsLoaded++;
                            else if (result == 0) duplicatesSkipped++;
                        }
                        batchSize = 0;
                    }
                    
                } catch (Exception e) {
                    context.getLogger().log("Error processing row: " + e.getMessage());
                }
            }
            
            // Execute remaining batch
            if (batchSize > 0) {
                int[] results = pstmt.executeBatch();
                for (int result : results) {
                    if (result > 0) rowsLoaded++;
                    else if (result == 0) duplicatesSkipped++;
                }
            }
            
            inspector.addTimeStamp("dataLoadEnd");
            
            // Cleanup
            pstmt.close();
            reader.close();
            
            // Add load metrics
            inspector.addAttribute("rowsLoaded", rowsLoaded);
            inspector.addAttribute("duplicatesSkipped", duplicatesSkipped);
            inspector.addAttribute("bucketName", bucketName);
            inspector.addAttribute("fileName", fileName);
            inspector.addAttribute("dbEndpoint", DB_ENDPOINT);
            inspector.addAttribute("dbName", DB_NAME);
            
        } catch (Exception e) {
            inspector.addAttribute("loadError", e.getMessage());
            context.getLogger().log("ERROR: " + e.getMessage());
            e.printStackTrace();
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
                "order_date VARCHAR(50), " +
                "order_id INT PRIMARY KEY, " +
                "ship_date VARCHAR(50), " +
                "units_sold INT, " +
                "unit_price DECIMAL(10,2), " +
                "unit_cost DECIMAL(10,2), " +
                "total_revenue DECIMAL(12,2), " +
                "total_cost DECIMAL(12,2), " +
                "total_profit DECIMAL(12,2), " +
                "order_processing_time INT, " +
                "gross_margin DECIMAL(5,4)" +
                ")";
        
        Statement stmt = conn.createStatement();
        stmt.execute(createTableSQL);
        stmt.close();
    }
    
    /**
     * Parse CSV line handling commas within quoted fields
     */
    private String[] parseCSVLine(String line) {
        // Simple CSV parser - handles basic cases
        // For production, consider using a library like OpenCSV
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }
}