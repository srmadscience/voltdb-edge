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

public class GetStatusMessage extends BaseMessage {

    String jsonPayload;

    public GetStatusMessage() {
        messageType = GET_STATUS;

    }

    public GetStatusMessage(long deviceId, long externallMessageId, long latencyMs, String errorMessage,
            Date createDate, int destinationSegmentId, long callingOwner, String jsonPayload) {

        super(deviceId, externallMessageId, GET_STATUS, latencyMs, errorMessage, createDate, destinationSegmentId,
                callingOwner);

        this.jsonPayload = jsonPayload;

    }

    @Override
    public StringBuffer asDelimited(String delimChar) {
        StringBuffer b = super.asDelimited(delimChar);

        b.append(jsonPayload);
        b.append(delimChar);

        return b;
    }

    public static GetStatusMessage fromDelimited(String c, String message) {

        String[] fields = message.split(c);

        String jsonPayload = fields[9];

        GetStatusMessage m = new GetStatusMessage();
        m.setInternals(fields);
        m.jsonPayload = jsonPayload;

        return m;
    }

    /**
     * @return the jsonPayload
     */
    public String getJsonPayload() {
        return jsonPayload;
    }

    /**
     * @param jsonPayload the jsonPayload to set
     */
    public void setJsonPayload(String jsonPayload) {
        this.jsonPayload = jsonPayload;
    }

    @Override
    public void setInternals(String[] internals) {

        super.setInternals(internals);

        jsonPayload = internals[9];

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("GetStatusMessage [jsonPayload=");
        builder.append(jsonPayload);
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

}
