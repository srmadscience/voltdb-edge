package org.voltse.edge.edgemessages;

import java.util.Date;

public class StartMessage extends BaseMessage {

    boolean started;

    public StartMessage() {
        messageType = START;
    }

    @Override
    public StringBuffer asDelimited(String delimChar) {
        StringBuffer b = super.asDelimited(delimChar);

        b.append(started);
        b.append(delimChar);

        return b;
    }

    public StartMessage(long deviceId, long externallMessageId, long latencyMs, String errorMessage,
            Date createDate, int destinationSegmentId, boolean started, long callingOwner) {

        super(deviceId, externallMessageId, START, latencyMs, errorMessage, createDate, destinationSegmentId,
                callingOwner);


        this.started = started;

    }

    public static StartMessage fromDelimited(String delimChar, String message) {

        String[] fields = message.split(delimChar + "");

        long deviceId = Long.parseLong(fields[0]);

        boolean started = false;

        if (fields[9].equals("true")) {
            started = true;
        }

        StartMessage m = new StartMessage();
        m.setInternals(fields);

        m.started = started;

        return m;
    }

    @Override
    public void setInternals(String[] internals) {

        super.setInternals(internals);

        started = false;

        if (internals[9].equals("true")) {
            started = true;
        }

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StartMessage [started=");
        builder.append(started);
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
     * @return the started
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * @param started the started to set
     */
    public void setStarted(boolean started) {
        this.started = started;
    }

    @Override
    public boolean isUpstreamOnly() {

        return true;
    }

}
