package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * AWS Lambda Query Function - Service #3
 * Runs read-only analytics queries on the sales_data table.
 *
 * This function is designed to:
 *  - Connect to the same Aurora MySQL database used by Load.java
 *  - Query the sales_data table created/filled by ExtractTransform.java + Load.java
 *  - Report simple aggregate metrics for comparison between Java and Python
 *
 * It can run:
 *  - Locally (via main) for testing on the VM
 *  - As an AWS Lambda (via handleRequest)
 *
 * Database connection information:
 *  - For Lambda: DB_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD (env vars)
 *  - For local testing: falls back to localhost/salesdb with root/password
 *
 * @author Team12
 */
public class Query implements RequestHandler<Map<String, Object>, HashMap<String, Object>> {

    // =========================================================
    // Local entry point (for EC2/VM testing)
    // =========================================================
    public static void main(String[] args) {
        try {
            // Ensure MySQL driver is loaded
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Try to read DB config from environment variables
            String endpoint = System.getenv("DB_ENDPOINT");
            String dbName   = System.getenv("DB_NAME");
            String user     = System.getenv("DB_USER");
            String password = System.getenv("DB_PASSWORD");

            // If env vars are missing, use a local default for quick testing
            if (endpoint == null || dbName == null || user == null || password == null) {
                endpoint = "localhost";
                dbName   = "salesdb";
                user     = "root";
                password = "password";
                System.out.println("[Query] Env vars not set, using LOCAL DB config...");
            }

            // Build JDBC connection string
            String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s", endpoint, dbName);
            System.out.println("[Query] Connecting to: " + jdbcUrl);

            long start = System.currentTimeMillis();
            runQueries(jdbcUrl, user, password);
            long end = System.currentTimeMillis();

            System.out.println();
            System.out.println("[Query] TOTAL RUNTIME: " + (end - start) + " ms");

        } catch (Exception e) {
            System.err.println("[Query] ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // =========================================================
    // AWS Lambda entry point
    // =========================================================
    @Override
    public HashMap<String, Object> handleRequest(Map<String, Object> input, Context context) {
        HashMap<String, Object> result = new HashMap<>();

        try {
            // Load MySQL driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            // DB configuration must come from Lambda environment variables
            String endpoint = System.getenv("DB_ENDPOINT");
            String dbName   = System.getenv("DB_NAME");
            String user     = System.getenv("DB_USER");
            String password = System.getenv("DB_PASSWORD");

            if (endpoint == null || dbName == null || user == null || password == null) {
                throw new RuntimeException("Missing DB env vars (DB_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD)");
            }

            String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s", endpoint, dbName);
            context.getLogger().log("[Query] Lambda connecting to: " + jdbcUrl + "\n");

            long start = System.currentTimeMillis();
            runQueries(jdbcUrl, user, password);
            long end = System.currentTimeMillis();

            // Simple JSON-style response for evaluation scripts
            result.put("status", "SUCCESS");
            result.put("runtime_ms", (end - start));
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("message", e.getMessage());
            e.printStackTrace();
        }

        return result;
    }

    // =========================================================
    // Shared query logic used by both main() and handleRequest()
    // =========================================================
    private static void runQueries(String jdbcUrl, String user, String password) throws SQLException {
        // sales_data schema is created by Load.java:
        //  region, country, item_type, sales_channel, order_priority,
        //  order_date (DATE), order_id (PK), ship_date (DATE),
        //  units_sold, unit_price, unit_cost, total_revenue, total_cost,
        //  total_profit, order_processing_time (INT), gross_margin (DECIMAL)
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            printTotalRevenueByRegion(conn);
            printAvgGrossMarginByRegion(conn);
            printAvgOrderProcessingTime(conn);
            printTop5CountriesByTotalProfit(conn);
        }
    }

    // =========================================================
    // Individual Queries
    // =========================================================

    /**
     * Query 1:
     * Total revenue grouped by region.
     */
    private static void printTotalRevenueByRegion(Connection conn) throws SQLException {
        String sql = """
            SELECT region, SUM(total_revenue) AS total_revenue
            FROM sales_data
            GROUP BY region
        """;

        System.out.println();
        System.out.println("=== TOTAL REVENUE BY REGION ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                String region = rs.getString("region");
                double totalRevenue = rs.getDouble("total_revenue");
                System.out.printf("Region: %-20s  Total Revenue: %.2f%n", region, totalRevenue);
            }
        }
    }

    /**
     * Query 2:
     * Average gross margin grouped by region.
     */
    private static void printAvgGrossMarginByRegion(Connection conn) throws SQLException {
        String sql = """
            SELECT region, AVG(gross_margin) AS avg_gm
            FROM sales_data
            GROUP BY region
        """;

        System.out.println();
        System.out.println("=== AVERAGE GROSS MARGIN BY REGION ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                String region = rs.getString("region");
                double avgGm = rs.getDouble("avg_gm");
                System.out.printf("Region: %-20s  Avg GM: %.4f%n", region, avgGm);
            }
        }
    }

    /**
     * Query 3:
     * Average order processing time (in days) across all orders.
     */
    private static void printAvgOrderProcessingTime(Connection conn) throws SQLException {
        String sql = "SELECT AVG(order_processing_time) AS avg_opt FROM sales_data";

        System.out.println();
        System.out.println("=== AVG ORDER PROCESSING TIME ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            if (rs.next()) {
                double avgOpt = rs.getDouble("avg_opt");
                System.out.printf("Average Days: %.2f%n", avgOpt);
            }
        }
    }

    /**
     * Query 4:
     * Top 5 countries ranked by total profit.
     */
    private static void printTop5CountriesByTotalProfit(Connection conn) throws SQLException {
        String sql = """
            SELECT country, SUM(total_profit) AS total_profit
            FROM sales_data
            GROUP BY country
            ORDER BY total_profit DESC
            LIMIT 5
        """;

        System.out.println();
        System.out.println("=== TOP 5 COUNTRIES BY TOTAL PROFIT ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                String country = rs.getString("country");
                double totalProfit = rs.getDouble("total_profit");
                System.out.printf("Country: %-20s  Total Profit: %.2f%n", country, totalProfit);
            }
        }
    }
}
