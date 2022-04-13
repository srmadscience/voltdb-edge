package org.voltse.edge.edgemessages;

import java.util.Date;

public class GetStatusMessage extends BaseMessage {

    String jsonPayload;

    public GetStatusMessage() {
        messageType = GET_STATUS;

    }

    public GetStatusMessage(long deviceId, long externallMessageId, long latencyMs, String errorMessage,
            Date createDate, int destinationSegmentId,  long callingOwner,String jsonPayload) {

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
