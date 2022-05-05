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

import com.google.gson.Gson;

public interface MessageIFace {

    String asJson(Gson g);

    StringBuffer asDelimited(String delimChar);

    public void setInternals(String[] internals);

    /**
     * @return the internalMessageId
     */
    long getInternalMessageId();

    /**
     * @param internalMessageId the internalMessageId to set
     */
    void setInternalMessageId(long internalMessageId);

    /**
     * @return the latencyMs
     */
    long getLatencyMs();

    /**
     * @param latencyMs the latencyMs to set
     */
    void setLatencyMs(long latencyMs);

    /**
     * @return the deviceId
     */
    long getDeviceId();

    /**
     * @return the externallMessageId
     */
    long getExternallMessageId();

    /**
     * @return the messageType
     */
    String getMessageType();

    /**
     * @return the errorMessage
     */
    String getErrorMessage();

    /**
     * @return the createDate
     */
    public Date getCreateDate();

    /**
     * @param createDate the createDate to set
     */
    public void setCreateDate(Date createDate);

    /**
     * @return the destinationSegmentId
     */
    public int getDestinationSegmentId();

    /**
     * @param destinationSegmentId the destinationSegmentId to set
     */
    public void setDestinationSegmentId(int destinationSegmentId);

    public void setCallingOwner(long callingOwner);

    public void setErrorMessage(String errorMessage);

    public boolean isUpstreamOnly();

}