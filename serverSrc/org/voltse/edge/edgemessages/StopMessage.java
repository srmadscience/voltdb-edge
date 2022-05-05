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

package org.voltse.edge.edgemessages;

import java.util.Date;

public class StopMessage extends BaseMessage {

    boolean stopped;

    public StopMessage() {
        messageType = STOP;

    }

    @Override
    public StringBuffer asDelimited(String delimChar) {
        StringBuffer b = super.asDelimited(delimChar);

        b.append(stopped);
        b.append(delimChar);

        return b;
    }

    public StopMessage(long deviceId, long externallMessageId, long latencyMs, String errorMessage, Date createDate,
            int destinationSegmentId, boolean stopped, long callingOwner) {

        super(deviceId, externallMessageId, STOP, latencyMs, errorMessage, createDate, destinationSegmentId,
                callingOwner);

        this.stopped = stopped;

    }

    public static StopMessage fromDelimited(String delimChar, String message) {

        String[] fields = message.split(delimChar + "");

        long deviceId = Long.parseLong(fields[0]);

        boolean stopped = false;

        if (fields[9].equals("true")) {
            stopped = true;
        }

        StopMessage m = new StopMessage();
        m.setInternals(fields);

        m.stopped = stopped;

        return m;
    }

    @Override
    public void setInternals(String[] internals) {

        super.setInternals(internals);

        stopped = false;

        if (internals[9].equals("true")) {
            stopped = true;
        }

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StopMessage [stopped=");
        builder.append(stopped);
        builder.append(", deviceId=");
        builder.append(deviceId);
        builder.append(", externallMessageId=");
        builder.append(externallMessageId);
        builder.append(", internalMessageId=");
        builder.append(internalMessageId);
        builder.append(", messageType=");
        builder.append(messageType);
        builder.append(", latencyMs=");
        builder.append(latencyMs);
        builder.append(", errorMessage=");
        builder.append(errorMessage);
        builder.append(", createDate=");
        builder.append(createDate);
        builder.append(", destinationSegmentId=");
        builder.append(destinationSegmentId);
        builder.append(", callingOwner=");
        builder.append(callingOwner);
        builder.append("]");
        return builder.toString();
    }

    /**
     * @return the stopped
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * @param stopped the stopped to set
     */
    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    @Override
    public boolean isUpstreamOnly() {

        return true;
    }

}
