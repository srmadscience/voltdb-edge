package org.voltse.edge.edgemessages;

import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import edgeprocs.ReferenceData;

public class BaseMessage implements MessageIFace {

    public static final String UPGRADE_FIRMWARE = "UpgradeFirmwareMessage";
    public static final String START = "StartMessage";
    public static final String STOP = "StopMessage";
    public static final String DISABLE_FEATURE = "DisableFeatureMessage";
    public static final String ENABLE_FEATURE = "EnableFeatureMessage";
    public static final String GET_STATUS = "GetStatusMessage";

    public long deviceId = 0;
    public long externallMessageId = 0;
    public long internalMessageId = 0;

    public String messageType;

    public long latencyMs = -1;

    public String errorMessage;

    public Date createDate;

    public int destinationSegmentId = 0;

    public long callingOwner = -1;

    public BaseMessage() {

    }

    public BaseMessage(long deviceId, long externallMessageId, String messageType, long latencyMs, String errorMessage,
            Date createDate, int destinationSegmentId, long callingOwner) {
        super();
        this.deviceId = deviceId;
        this.externallMessageId = externallMessageId;
        this.messageType = messageType;
        this.latencyMs = latencyMs;
        this.errorMessage = errorMessage;
        this.createDate = createDate;
        this.destinationSegmentId = destinationSegmentId;
        this.callingOwner = callingOwner;
    }

    @Override
    public String asJson(Gson g) {
        return g.toJson(this);
    }

    @Override
    public StringBuffer asDelimited(String delimChar) {

        StringBuffer b = new StringBuffer();
        b.append(deviceId);
        b.append(delimChar);
        b.append(internalMessageId);
        b.append(delimChar);
        b.append(externallMessageId);
        b.append(delimChar);
        b.append(messageType);
        b.append(delimChar);
        b.append(latencyMs);
        b.append(delimChar);
        if (errorMessage != null) {
            b.append(errorMessage);
        }
        b.append(delimChar);
        if (createDate != null) {
            b.append(createDate.getTime());
        }

        b.append(delimChar);
        b.append(destinationSegmentId);
        b.append(delimChar);
        b.append(callingOwner);
        b.append(delimChar);

        return b;

    }

    @Override
    public void setInternals(String[] internals) {

        deviceId = Long.parseLong(internals[0]);
        internalMessageId = Long.parseLong(internals[1]);
        deviceId = Long.parseLong(internals[0]);
        externallMessageId = Long.parseLong(internals[2]);
        messageType = internals[3];
        latencyMs = Long.parseLong(internals[4]);
        
        if (internals[5] != null && internals[5].length() > 0) {
            errorMessage = internals[5];
        } else {
            errorMessage = null;
        }
  
        if (internals[6] != null && internals[6].length() > 0) {
            createDate = new Date(Long.parseLong(internals[6]));
        } else {
            createDate = null;
        }

        destinationSegmentId = Integer.parseInt(internals[7]);
        callingOwner = Long.parseLong(internals[8]);
    }

    public static String delimitedMessageType(String delimChar, String message) {

        String[] fields = message.split(delimChar);

        return fields[3];
    }

    public static Object fromJson(String mAsJson, Gson g) throws JsonSyntaxException, ClassNotFoundException {
        BaseMessage baseObject = g.fromJson(mAsJson, BaseMessage.class);

        Object newObject = g.fromJson(mAsJson, Class.forName(ReferenceData.EDGEMESSAGES + baseObject.messageType));
        return newObject;

    }

    /**
     * @return the internalMessageId
     */
    @Override
    public long getInternalMessageId() {
        return internalMessageId;
    }

    /**
     * @param internalMessageId the internalMessageId to set
     */
    @Override
    public void setInternalMessageId(long internalMessageId) {
        this.internalMessageId = internalMessageId;
    }

    /**
     * @return the latencyMs
     */
    @Override
    public long getLatencyMs() {
        return latencyMs;
    }

    /**
     * @param latencyMs the latencyMs to set
     */
    @Override
    public void setLatencyMs(long latencyMs) {
        this.latencyMs = latencyMs;
    }

    /**
     * @return the upgradeFirmware
     */
    public static String getUpgradeFirmware() {
        return UPGRADE_FIRMWARE;
    }

    /**
     * @return the start
     */
    public static String getStart() {
        return START;
    }

    /**
     * @return the stop
     */
    public static String getStop() {
        return STOP;
    }

    /**
     * @return the disableFeature
     */
    public static String getDisableFeature() {
        return DISABLE_FEATURE;
    }

    /**
     * @return the enableFeature
     */
    public static String getEnableFeature() {
        return ENABLE_FEATURE;
    }

    /**
     * @return the getStatus
     */
    public static String getGetStatus() {
        return GET_STATUS;
    }

    /**
     * @return the deviceId
     */
    @Override
    public long getDeviceId() {
        return deviceId;
    }

    /**
     * @return the externallMessageId
     */
    @Override
    public long getExternallMessageId() {
        return externallMessageId;
    }

    /**
     * @return the messageType
     */
    @Override
    public String getMessageType() {
        return messageType;
    }

    /**
     * @return the errorMessage
     */
    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * @return the createDate
     */
    @Override
    public Date getCreateDate() {
        return createDate;
    }

    /**
     * @param createDate the createDate to set
     */
    @Override
    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    /**
     * @return the destinationSegmentId
     */
    @Override
    public int getDestinationSegmentId() {
        return destinationSegmentId;
    }

    /**
     * @param destinationSegmentId the destinationSegmentId to set
     */
    @Override
    public void setDestinationSegmentId(int destinationSegmentId) {
        this.destinationSegmentId = destinationSegmentId;
    }

    /**
     * @param deviceId the deviceId to set
     */
    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * @param externallMessageId the externallMessageId to set
     */
    public void setExternallMessageId(long externallMessageId) {
        this.externallMessageId = externallMessageId;
    }

    /**
     * @param messageType the messageType to set
     */
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    /**
     * @param errorMessage the errorMessage to set
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * @return the callingOwner
     */
    public long getCallingOwner() {
        return callingOwner;
    }

    /**
     * @param callingOwner the callingOwner to set
     */
    @Override
    public void setCallingOwner(long callingOwner) {
        this.callingOwner = callingOwner;
    }

    @Override
    public boolean isUpstreamOnly() {
   
        return false;
    }

}
