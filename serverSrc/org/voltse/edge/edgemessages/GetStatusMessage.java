package org.voltse.edge.edgemessages;

public class GetStatusMessage extends BaseMessage {

    String jsonPayload;

    public GetStatusMessage() {
        messageType = GET_STATUS;

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

        String jsonPayload = fields[8];

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

        jsonPayload = internals[8];

    }


}
