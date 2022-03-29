package org.voltse.edge.edgeencoders;

import org.voltse.edge.edgemessages.MessageIFace;

public interface ModelEncoderIFace {

    public String encode(MessageIFace m) throws Exception ;

    public MessageIFace decode(String s)  throws Exception;



}
