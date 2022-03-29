package org.voltse.edge.edgemessages;

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

    public static StartMessage fromDelimited(String delimChar, String message) {

        String[] fields = message.split(delimChar + "");

        long deviceId = Long.parseLong(fields[0]);

        boolean started = false;

        if (fields[8].equals("true")) {
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

        if (internals[8].equals("true")) {
            started = true;
        }



    }
}
