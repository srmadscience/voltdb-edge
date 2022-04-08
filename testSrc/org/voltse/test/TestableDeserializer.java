package org.voltse.test;

import org.apache.kafka.common.serialization.Deserializer;
import org.voltse.edge.edgeencoders.TabEncoderImpl;
import org.voltse.edge.edgemessages.MessageIFace;

public class TestableDeserializer implements Deserializer<MessageIFace> {

    TabEncoderImpl i = new TabEncoderImpl();

    @Override
    public MessageIFace deserialize(String arg0, byte[] arg1) {

        try {
            return i.decode(new String(arg1));
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        return null;

    }

}
