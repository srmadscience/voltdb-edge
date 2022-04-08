package org.voltse.edge.edgemessages;

public class DisableFeatureMessage extends BaseMessage {

    public boolean enabled;

    public String featureName;

    public DisableFeatureMessage() {
        messageType = DISABLE_FEATURE;
    }

    @Override
    public StringBuffer asDelimited(String delimChar) {
        StringBuffer b = super.asDelimited(delimChar);

        b.append(enabled);
        b.append(delimChar);

        b.append(featureName);
        b.append(delimChar);

        return b;
    }

    public static DisableFeatureMessage fromDelimited(String delimChar, String message) {

        String[] fields = message.split(delimChar);

        long deviceId = Long.parseLong(fields[0]);

        boolean enabled = false;

        if (fields[9].equals("true")) {
            enabled = true;
        }

        String featureName = fields[10];

        DisableFeatureMessage m = new DisableFeatureMessage();
        m.setInternals(fields);

        m.enabled = enabled;

        return m;
    }

    @Override
    public void setInternals(String[] internals) {

        super.setInternals(internals);

        enabled = false;

        if (internals[9].equals("true")) {
            enabled = true;
        }

        featureName = internals[10];

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DisableFeatureMessage [enabled=");
        builder.append(enabled);
        builder.append(", featureName=");
        builder.append(featureName);
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
