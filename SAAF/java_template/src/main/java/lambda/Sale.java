package lambda;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class Sale {
    String region;
    String country;
    String itemType;
    String salesChannel;
    String orderPriority;
    LocalDate orderDate;
    int orderId;
    LocalDate shipDate;
    int unitsSold;
    BigDecimal unitPrice;
    BigDecimal unitCost;
    BigDecimal totalRevenue;
    BigDecimal totalCost;
    BigDecimal totalProfit;

    // New Variables
    int orderProcessingTime;
    BigDecimal grossMargin;

    public Sale() {
    }

    public Sale(String region, String country, String itemType, String salesChannel, String orderPriority, LocalDate orderDate, int orderId, LocalDate shipDate, int unitsSold, BigDecimal unitPrice, BigDecimal unitCost, BigDecimal totalRevenue, BigDecimal totalCost, BigDecimal totalProfit) {
        this.region = region;
        this.country = country;
        this.itemType = itemType;
        this.salesChannel = salesChannel;

        this.orderPriority = formatPriority(orderPriority);

        this.orderDate = orderDate;
        this.orderId = orderId;
        this.shipDate = shipDate;
        this.unitsSold = unitsSold;
        this.unitPrice = unitPrice;
        this.unitCost = unitCost;
        this.totalRevenue = totalRevenue;
        this.totalCost = totalCost;
        this.totalProfit = totalProfit;

        if (orderDate != null && shipDate != null) {
            this.orderProcessingTime = Math.toIntExact(ChronoUnit.DAYS.between(orderDate, shipDate));
        }

        this.grossMargin = totalProfit.divide(totalRevenue);
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public String getSalesChannel() {
        return salesChannel;
    }

    public void setSalesChannel(String salesChannel) {
        this.salesChannel = salesChannel;
    }

    public String getOrderPriority() {
        return orderPriority;
    }

    public void setOrderPriority(String orderPriority) {
        this.orderPriority = orderPriority;
    }

    public LocalDate getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(LocalDate orderDate) {
        this.orderDate = orderDate;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public LocalDate getShipDate() {
        return shipDate;
    }

    public void setShipDate(LocalDate shipDate) {
        this.shipDate = shipDate;
    }

    public int getUnitsSold() {
        return unitsSold;
    }

    public void setUnitsSold(int unitsSold) {
        this.unitsSold = unitsSold;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public BigDecimal getUnitCost() {
        return unitCost;
    }

    public void setUnitCost(BigDecimal unitCost) {
        this.unitCost = unitCost;
    }

    public BigDecimal getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(BigDecimal totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    public BigDecimal getTotalCost() {
        return totalCost;
    }

    public void setTotalCost(BigDecimal totalCost) {
        this.totalCost = totalCost;
    }

    public BigDecimal getTotalProfit() {
        return totalProfit;
    }

    public void setTotalProfit(BigDecimal totalProfit) {
        this.totalProfit = totalProfit;
    }

    private String formatPriority(String priority) {
        if (priority != null) {
            switch (priority) {
                case "L":
                    return "Low";
                case "M":
                    return "Medium";
                case "H":
                    return "High";
                case "C":
                    return "Critical";
            }
        }

        return "";
    }
}
