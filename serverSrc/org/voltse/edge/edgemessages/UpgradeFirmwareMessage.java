package org.voltse.edge.edgemessages;

import java.util.Base64;

public class UpgradeFirmwareMessage extends BaseMessage {

    byte[] payload;

    boolean enabled;

    String message;

    public UpgradeFirmwareMessage() {
        messageType = UPGRADE_FIRMWARE;
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


    m.setPayload(Base64.getDecoder().decode(fields[8]));

    m.setEnabled(false);

    if (fields[9].equals("true")) {
        m.setEnabled(true);
    }
    m.setMessage(fields[10]) ;

    return m;
}

@Override
public void setInternals(String[] internals) {

    super.setInternals(internals);

    setPayload(Base64.getDecoder().decode(internals[8]));

    enabled = false;

    if (internals[9].equals("true")) {
        enabled = true;
    }

    message = internals[10];

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
}
