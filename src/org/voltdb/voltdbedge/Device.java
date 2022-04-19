package org.voltdb.voltdbedge;

import java.util.ArrayList;

import org.voltse.edge.edgeencoders.ModelEncoderIFace;
import org.voltse.edge.edgemessages.MessageIFace;

public class Device {
    
    public Device(long deviceId, ModelEncoderIFace encoder) {
        super();
        this.deviceId = deviceId;
        this.encoder = encoder;
    }

    long deviceId;
    
    ModelEncoderIFace encoder;
    
    ArrayList<MessageIFace> messages = new ArrayList<MessageIFace>();

    /**
     * @return the deviceId
     */
    public long getDeviceId() {
        return deviceId;
    }

    /**
     * @param deviceId the deviceId to set
     */
    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * @return the encoder
     */
    public ModelEncoderIFace getEncoder() {
        return encoder;
    }

    /**
     * @param encoder the encoder to set
     */
    public void setEncoder(ModelEncoderIFace encoder) {
        this.encoder = encoder;
    }

}
