package org.voltse.edge.edgeencoders;

import org.voltse.edge.edgemessages.BaseMessage;
import org.voltse.edge.edgemessages.MessageIFace;

import com.google.gson.Gson;

public class JsonEncoderImpl implements ModelEncoderIFace {

    Gson g = new Gson();

    @Override
    public String encode(MessageIFace m) throws Exception {
        return m.asJson(g);
    }

    @Override
    public MessageIFace decode(String s) throws Exception {

        return (MessageIFace) BaseMessage.fromJson(s, g);
    }

}
