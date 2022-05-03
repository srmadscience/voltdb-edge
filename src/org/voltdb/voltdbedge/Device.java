package org.voltdb.voltdbedge;

import java.util.ArrayList;
import java.util.HashMap;

import org.voltse.edge.edgeencoders.ModelEncoderIFace;
import org.voltse.edge.edgemessages.MessageIFace;

public class Device {

    long deviceId;

    ModelEncoderIFace encoder;

    String modelNumber;

    ArrayList<MessageIFace> messages = new ArrayList<>();

    HashMap<String, Boolean> features = new HashMap<>();

    long meterReading = 0;

    public Device(long deviceId, ModelEncoderIFace encoder, String modelNumber) {
        super();
        this.deviceId = deviceId;
        this.encoder = encoder;
        this.modelNumber = modelNumber;
    }

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

    /**
     * @return the modelNumber
     */
    public String getModelNumber() {
        return modelNumber;
    }

    /**
     * @param modelNumber the modelNumber to set
     */
    public void setModelNumber(String modelNumber) {
        this.modelNumber = modelNumber;
    }

    public void addMessage(MessageIFace message) {
        messages.add(message);

    }

    /**
     * @return the meterReading
     */
    public long getMeterReading() {
        return meterReading;
    }

    /**
     * @param meterReading the meterReading to set
     */
    public void setMeterReading(long meterReading) {
        this.meterReading = meterReading;
    }

    @SuppressWarnings("deprecation")
    public void setFeature(String featureName, boolean set) {
        features.put(featureName, new Boolean(set));
    }

    public boolean getFeature(String featureName) {
        Boolean isEnabled = features.get(featureName);

        if (isEnabled == null) {
            return false;
        }

        return isEnabled;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Device [deviceId=");
        builder.append(deviceId);
        builder.append(", encoder=");
        builder.append(encoder);
        builder.append(", modelNumber=");
        builder.append(modelNumber);
        builder.append(", messages=");
        builder.append(messages);
        builder.append(", features=");
        builder.append(features);
        builder.append(", meterReading=");
        builder.append(meterReading);
        builder.append("]");
        return builder.toString();
    }

}
