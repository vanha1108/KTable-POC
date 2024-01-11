package org.example.demo.materializedviewmaintenance;

import java.io.Serializable;

public class UserBalance implements Serializable {

    private double totalBalance;

    @Override
    public String toString() {
        return "UserBalance{" +
                "totalBalance=" + totalBalance +
                '}';
    }

    public UserBalance() {
        this.totalBalance = 0.0;
    }

    public double getTotalBalance() {
        return totalBalance;
    }

    public void updateBalance(double amount) {
        this.totalBalance += amount;
    }
}
