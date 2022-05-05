/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

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
