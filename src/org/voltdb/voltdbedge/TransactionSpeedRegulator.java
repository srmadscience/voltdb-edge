package org.voltdb.voltdbedge;

public class TransactionSpeedRegulator {

    public static final long NO_END_DATE = Long.MIN_VALUE;
    int tpThisMs = 0;
    int tpMs;

    long currentMs = 0;
    long endtimeMs = 0;

    public TransactionSpeedRegulator(int tpMs, long endtimeMs) {
        this.tpMs = tpMs;
        this.endtimeMs = endtimeMs;
    }

    public boolean waitIfNeeded() {

        if (endtimeMs != NO_END_DATE && endtimeMs < System.currentTimeMillis()) {
            return false;
        }

        if (tpThisMs++ > tpMs) {

            while (currentMs == System.currentTimeMillis()) {
                try {
                    Thread.sleep(0, 50000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            currentMs = System.currentTimeMillis();
            tpThisMs = 0;
        }

        return true;
    }
}
