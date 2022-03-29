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

}