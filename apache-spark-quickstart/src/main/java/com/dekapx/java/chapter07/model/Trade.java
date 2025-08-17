package com.dekapx.java.chapter07.model;

import java.util.Date;

public class Trade {
    private Long tradeId;
    private String type;
    private Double price;
    private Integer quantity;
    private String location;
    private Date tradeDate;

    public Trade(Long tradeId, String type, Double price, Integer quantity, String location, Date tradeDate) {
        this.tradeId = tradeId;
        this.type = type;
        this.price = price;
        this.quantity = quantity;
        this.location = location;
        this.tradeDate = tradeDate;
    }

    public Long getTradeId() {
        return tradeId;
    }
    public String getType() {
        return type;
    }
    public Double getPrice() {
        return price;
    }
    public Integer getQuantity() {
        return quantity;
    }
    public String getLocation() {
        return location;
    }
    public Date getTradeDate() {
        return tradeDate;
    }

    // generate toString method
    @Override
    public String toString() {
        return "Trade{" +
                "tradeId=" + tradeId +
                ", type='" + type + '\'' +
                ", price=" + price +
                ", quantity=" + quantity +
                ", location='" + location + '\'' +
                ", tradeDate=" + tradeDate +
                '}';
    }
}
