package org.example.demo.windowedaggregation;

public class TemperatureAggregate {

    private double sum;
    private int count;

    @Override
    public String toString() {
        return "TemperatureAggregate{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }

    public TemperatureAggregate() {
        sum = 0.0;
        count = 0;
    }

    public void addTemperature(double temperature) {
        sum += temperature;
        count++;
    }

    public double showAverageTemperature() {
        return count == 0 ? 0.0 : sum / count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
