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

import java.util.Arrays;
import java.util.Base64;
import java.util.Date;

public class UpgradeFirmwareMessage extends BaseMessage {

    byte[] payload;

    boolean enabled;

    String message;

    public UpgradeFirmwareMessage() {
        messageType = UPGRADE_FIRMWARE;
    }

    public UpgradeFirmwareMessage(long deviceId, long externallMessageId, long latencyMs, String errorMessage,
            Date createDate, int destinationSegmentId, byte[] payload, long callingOwner) {

        super(deviceId, externallMessageId, UPGRADE_FIRMWARE, latencyMs, errorMessage, createDate, destinationSegmentId,
                callingOwner);

        this.payload = payload;

    }

    /**
     * @return the payload
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * @param payload the payload to set
     */
    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public StringBuffer asDelimited(String delimChar) {
        StringBuffer b = super.asDelimited(delimChar);

        b.append(Base64.getEncoder().encodeToString(payload));
        b.append(delimChar);
        b.append(enabled);
        b.append(delimChar);
        b.append(message);
        b.append(delimChar);

        return b;
    }

    public static UpgradeFirmwareMessage fromDelimited(String delimChar, String message) {

        String[] fields = message.split(delimChar);

        UpgradeFirmwareMessage m = new UpgradeFirmwareMessage();
        m.setInternals(fields);

        m.setPayload(Base64.getDecoder().decode(fields[9]));

        m.setEnabled(false);

        if (fields[10].equals("true")) {
            m.setEnabled(true);
        }
        m.setMessage(fields[11]);

        return m;
    }

    @Override
    public void setInternals(String[] internals) {

        super.setInternals(internals);

        setPayload(Base64.getDecoder().decode(internals[9]));

        enabled = false;

        if (internals[10].equals("true")) {
            enabled = true;
        }

        message = internals[11];

    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param message the message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("UpgradeFirmwareMessage [payload=");
        builder.append(Arrays.toString(payload));
        builder.append(", enabled=");
        builder.append(enabled);
        builder.append(", message=");
        builder.append(message);
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
