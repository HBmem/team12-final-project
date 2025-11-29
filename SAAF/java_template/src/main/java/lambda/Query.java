package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Query
 *
 * Java Lambda that runs summary queries against the sales_data table.
 * - main() is for local testing.
 * - handleRequest(...) is the Lambda entry point.
 */
public class Query implements RequestHandler<Map<String, Object>, HashMap<String, Object>> {

    // ========================
    // LOCAL MAIN (for testing)
    // ========================
    public static void main(String[] args) {
        try {
            // Make sure the MySQL driver is available
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Try to read connection info from env vars
            String endpoint = System.getenv("DB_ENDPOINT");
            String dbName   = System.getenv("DB_NAME");
            String user     = System.getenv("DB_USER");
            String password = System.getenv("DB_PASSWORD");

            // Fallback to a local DB if env vars are not set
            if (endpoint == null || dbName == null || user == null || password == null) {
                endpoint = "localhost";
                dbName   = "salesdb";
                user     = "root";
                password = "password";
                System.out.println("[Query] Env vars not set, using LOCAL DB config...");
            }

            // Build JDBC URL and run the queries
            String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s", endpoint, dbName);
            System.out.println("[Query] Connecting to: " + jdbcUrl);

            long start = System.currentTimeMillis();
            runQueries(jdbcUrl, user, password);
            long end = System.currentTimeMillis();

            System.out.println("\n[Query] TOTAL RUNTIME: " + (end - start) + " ms");

        } catch (Exception e) {
            System.err.println("[Query] ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ========================
    // LAMBDA HANDLER
    // ========================
    @Override
    public HashMap<String, Object> handleRequest(Map<String, Object> input, Context context) {
        HashMap<String, Object> result = new HashMap<>();

        try {
            // Load MySQL driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Read DB connection info from Lambda env vars
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

            // Simple JSON-style response for the inspector / logs
            result.put("status", "SUCCESS");
            result.put("runtime_ms", (end - start));
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("message", e.getMessage());
            e.printStackTrace();
        }

        return result;
    }

    // ===============================================================
    // Main Query Logic – shared by local and Lambda
    // ===============================================================
    private static void runQueries(String jdbcUrl, String user, String password) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            printTotalRevenueByRegion(conn);
            printAvgGrossMarginByRegion(conn);
            printAvgOrderProcessingTime(conn);
            printTop5CountriesByTotalProfit(conn);
        }
    }

    // 1) Total revenue by region
    private static void printTotalRevenueByRegion(Connection conn) throws SQLException {
        String sql = """
            SELECT region, SUM(total_revenue) AS total_revenue
            FROM sales_data
            GROUP BY region
        """;

        System.out.println("\n=== TOTAL REVENUE BY REGION ===");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("Region: %-15s  Total Revenue: %.2f%n",
                        rs.getString("region"),
                        rs.getDouble("total_revenue"));
            }
        }
    }

    // 2) Average gross margin by region
    private static void printAvgGrossMarginByRegion(Connection conn) throws SQLException {
        String sql = """
            SELECT region, AVG(gross_margin) AS avg_gm
            FROM sales_data
            GROUP BY region
        """;

        System.out.println("\n=== AVERAGE GROSS MARGIN BY REGION ===");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("Region: %-15s  Avg GM: %.4f%n",
                        rs.getString("region"),
                        rs.getDouble("avg_gm"));
            }
        }
    }

    // 3) Average order processing time (days)
    private static void printAvgOrderProcessingTime(Connection conn) throws SQLException {
        String sql = "SELECT AVG(order_processing_time) AS avg_opt FROM sales_data";

        System.out.println("\n=== AVG ORDER PROCESSING TIME ===");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                System.out.printf("Average Days: %.2f%n", rs.getDouble("avg_opt"));
            }
        }
    }

    // 4) Top 5 countries ranked by total profit
    private static void printTop5CountriesByTotalProfit(Connection conn) throws SQLException {
        String sql = """
            SELECT country, SUM(total_profit) AS total_profit
            FROM sales_data
            GROUP BY country
            ORDER BY total_profit DESC
            LIMIT 5
        """;

        System.out.println("\n=== TOP 5 COUNTRIES BY TOTAL PROFIT ===");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("Country: %-15s  Total Profit: %.2f%n",
                        rs.getString("country"),
                        rs.getDouble("total_profit"));
            }
        }
    }
}
