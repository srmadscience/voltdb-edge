package org.voltse.edge.edgemessages;

public class EnableFeatureMessage extends BaseMessage {

    public boolean enabled;

    public String featureName;

    public EnableFeatureMessage() {
        messageType = ENABLE_FEATURE;
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

    public static EnableFeatureMessage fromDelimited(String delimChar, String message) {

        String[] fields = message.split(delimChar);

        long deviceId = Long.parseLong(fields[0]);

        boolean enabled = false;

        if (fields[8].equals("true")) {
            enabled = true;
        }

        String featureName = fields[9];

        EnableFeatureMessage m = new EnableFeatureMessage();
        m.setInternals(fields);

        m.enabled = enabled;

        return m;
    }

    @Override
    public void setInternals(String[] internals) {

        super.setInternals(internals);

         enabled = false;

        if (internals[8].equals("true")) {
            enabled = true;
        }

         featureName = internals[9];

    }


}
