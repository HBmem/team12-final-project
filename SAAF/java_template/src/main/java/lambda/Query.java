package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;

import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

/**
 * AWS Lambda Query Function - Service #3
 * Runs analytical SQL queries on the sales_data table in Aurora MySQL.
 *
 * @author Team12
 */
public class Query implements RequestHandler<Request, HashMap<String, Object>> {

    // Database connection parameters loaded from db.properties
    private String url;
    private String username;
    private String password;

    /**
     * Load database configuration from db.properties file
     * (same pattern as Load.java)
     */
    private void loadDatabaseConfig() throws Exception {
        Properties prop = new Properties();
        InputStream input = Query.class.getClassLoader().getResourceAsStream("db.properties");

        if (input == null) {
            throw new Exception("Unable to find db.properties");
        }

        prop.load(input);

        url = prop.getProperty("url");
        username = prop.getProperty("username");
        password = prop.getProperty("password");

        input.close();
    }

    /**
     * Optional local main for testing outside Lambda
     */
    public static void main(String[] args) {
        Query q = new Query();
        try {
            q.loadDatabaseConfig();
            Class.forName("com.mysql.cj.jdbc.Driver");

            System.out.println("[Query] Connecting locally to: " + q.url);
            long start = System.currentTimeMillis();

            try (Connection conn = DriverManager.getConnection(q.url, q.username, q.password)) {
                q.runQueries(conn);
            }

            long end = System.currentTimeMillis();
            System.out.println("[Query] Runtime: " + (end - start) + " ms");
        } catch (Exception e) {
            System.err.println("[Query] ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        Connection conn = null;

        try {
            // Load DB config from db.properties
            loadDatabaseConfig();

            // Connect to Aurora MySQL
            inspector.addTimeStamp("dbConnectionStart");
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
            inspector.addTimeStamp("dbConnectionEnd");
            context.getLogger().log("[Query] Connected to Aurora MySQL\n");

            // Run analytical queries
            inspector.addTimeStamp("queryStart");
            runQueries(conn);
            inspector.addTimeStamp("queryEnd");

            // Build simple response
            Response response = new Response();
            response.setValue("Query service completed successfully against: " + url);
            inspector.consumeResponse(response);

        } catch (Exception e) {
            inspector.addAttribute("queryError", e.getMessage());
            context.getLogger().log("ERROR (Query): " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close connection
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception e) {
                context.getLogger().log("Error closing connection in Query: " + e.getMessage());
            }
        }

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    /**
     * Runs all query statements against the sales_data table.
     */
    private void runQueries(Connection conn) throws SQLException {
        printTotalRevenueByRegion(conn);
        printAvgGrossMarginByRegion(conn);
        printAvgOrderProcessingTime(conn);
        printTop5CountriesByTotalProfit(conn);
    }

    // 1) TOTAL REVENUE BY REGION
    private void printTotalRevenueByRegion(Connection conn) throws SQLException {
        String sql =
                "SELECT region, SUM(total_revenue) AS total_revenue " +
                        "FROM sales_data " +
                        "GROUP BY region " +
                        "ORDER BY region";

        System.out.println("\n=== TOTAL REVENUE BY REGION ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                String region = rs.getString("region");
                double revenue = rs.getDouble("total_revenue");
                System.out.printf("Region: %-15s | Total Revenue: %.2f%n", region, revenue);
            }
        }
    }

    // 2) AVERAGE GROSS MARGIN BY REGION
    private void printAvgGrossMarginByRegion(Connection conn) throws SQLException {
        String sql =
                "SELECT region, AVG(gross_margin) AS avg_gm " +
                        "FROM sales_data " +
                        "GROUP BY region " +
                        "ORDER BY region";

        System.out.println("\n=== AVERAGE GROSS MARGIN BY REGION ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                String region = rs.getString("region");
                double avgGm = rs.getDouble("avg_gm");
                System.out.printf("Region: %-15s | Avg Gross Margin: %.4f%n", region, avgGm);
            }
        }
    }

    // 3) AVERAGE ORDER PROCESSING TIME
    private void printAvgOrderProcessingTime(Connection conn) throws SQLException {
        String sql =
                "SELECT AVG(order_processing_time) AS avg_opt " +
                        "FROM sales_data";

        System.out.println("\n=== AVG ORDER PROCESSING TIME (DAYS) ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            if (rs.next()) {
                double avgDays = rs.getDouble("avg_opt");
                System.out.printf("Average Order Processing Time: %.2f days%n", avgDays);
            }
        }
    }

    // 4) TOP 5 COUNTRIES BY TOTAL PROFIT
    private void printTop5CountriesByTotalProfit(Connection conn) throws SQLException {
        String sql =
                "SELECT country, SUM(total_profit) AS total_profit " +
                        "FROM sales_data " +
                        "GROUP BY country " +
                        "ORDER BY total_profit DESC " +
                        "LIMIT 5";

        System.out.println("\n=== TOP 5 COUNTRIES BY TOTAL PROFIT ===");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                String country = rs.getString("country");
                double profit = rs.getDouble("total_profit");
                System.out.printf("Country: %-15s | Total Profit: %.2f%n", country, profit);
            }
        }
    }
}
