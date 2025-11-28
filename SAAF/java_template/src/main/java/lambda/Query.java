package lambda;

import java.sql.*;

/**
 * SERVICE #3 – QUERY
 * Reads the "sales_data" table in Aurora MySQL (created by Service #2 / Load)
 * and prints analytical results that we can show to the professor.
 *
 * Works for LOCAL and LAMBDA – no extra files needed.
 */
public class Query {

    public static void main(String[] args) {
        try {
            // === Same environment variables used by Load.java ===
            String endpoint = System.getenv("DB_ENDPOINT");
            String dbName   = System.getenv("DB_NAME");
            String user     = System.getenv("DB_USER");
            String password = System.getenv("DB_PASSWORD");

            // ---- Local backup values for testing (CHANGE THESE if needed) ----
            if (endpoint == null || dbName == null || user == null || password == null) {
                endpoint = "localhost";     // <--------- update if using local DB
                dbName   = "salesdb";
                user     = "root";
                password = "password";
                System.out.println("[Query] Env vars not set, using LOCAL DB config...");
            }

            // JDBC connection string
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

    // ===============================================================
    // Main Query Logic
    // ===============================================================
    private static void runQueries(String jdbcUrl, String user, String password) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            printTotalRevenueByRegion(conn);
            printAvgGrossMarginByRegion(conn);
            printAvgOrderProcessingTime(conn);
            printTop5CountriesByTotalProfit(conn);
        }
    }

    // 1) TOTAL REVENUE BY REGION
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

    // 2) AVG GROSS MARGIN BY REGION
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

    // 3) AVG ORDER PROCESSING TIME
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

    // 4) TOP 5 COUNTRIES BY TOTAL PROFIT
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
