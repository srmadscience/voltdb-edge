package org.voltse.edge.edgemessages;

import java.util.Date;

public class DisableFeatureMessage extends BaseMessage {

    public boolean enabled;

    public String featureName;

    public DisableFeatureMessage() {
        messageType = DISABLE_FEATURE;
    }

    public DisableFeatureMessage(long deviceId, long externallMessageId, long latencyMs, String errorMessage,
            Date createDate, int destinationSegmentId, String featureName, boolean enabled, long callingOwner) {

        super(deviceId, externallMessageId, DISABLE_FEATURE, latencyMs, errorMessage, createDate, destinationSegmentId,
                callingOwner);

        this.featureName = featureName;
        this.enabled = enabled;

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

    /**
     * @return the featureName
     */
    public String getFeatureName() {
        return featureName;
    }

    /**
     * @param featureName the featureName to set
     */
    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

}
