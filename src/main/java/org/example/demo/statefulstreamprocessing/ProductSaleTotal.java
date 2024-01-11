package org.example.demo.statefulstreamprocessing;

public class ProductSaleTotal {

    private int totalQuantity;

    @Override
    public String toString() {
        return "ProductSaleTotal{" +
                "totalQuantity=" + totalQuantity +
                '}';
    }

    public void updateTotalQuantity(int quantity) {
        this.totalQuantity += quantity;
    }

    public int getTotalQuantity() {
        return totalQuantity;
    }

    public void setTotalQuantity(int totalQuantity) {
        this.totalQuantity = totalQuantity;
    }
}
