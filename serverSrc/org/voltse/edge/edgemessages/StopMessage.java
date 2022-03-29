package org.voltse.edge.edgemessages;

public class StopMessage extends BaseMessage {

    boolean stopped;

    public StopMessage() {
        messageType = STOP;

    }

    @Override
    public StringBuffer asDelimited(String delimChar) {
        StringBuffer b = super.asDelimited(delimChar);

        b.append(stopped);
        b.append(delimChar);

        return b;
    }

    public static StopMessage fromDelimited(String delimChar, String message) {

        String[] fields = message.split(delimChar + "");

        long deviceId = Long.parseLong(fields[0]);

        boolean stopped = false;

        if (fields[8].equals("true")) {
            stopped = true;
        }

        StopMessage m = new StopMessage();
        m.setInternals(fields);

        m.stopped = stopped;

        return m;
    }

    @Override
    public void setInternals(String[] internals) {

        super.setInternals(internals);

        stopped = false;

        if (internals[8].equals("true")) {
            stopped = true;
        }



    }

}
