package org.voltdb.voltdbedge;

public class LogicalDeviceGenericResponse {

    long downstreamRequestId;

    long deviceId;

    int messageType;

    String errorMessage;

    boolean ok;

    String jsonStatus;

}
