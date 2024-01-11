package org.example.demo.materializedviewmaintenance;

public class TransactionEvent {

    private String userId;
    private double amount;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public TransactionEvent() {
    }

    public TransactionEvent(String userId, double amount) {
        this.userId = userId;
        this.amount = amount;
    }
}
