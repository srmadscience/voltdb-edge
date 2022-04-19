package org.voltse.edge.edgeencoders;

import org.voltse.edge.edgemessages.BaseMessage;
import org.voltse.edge.edgemessages.MessageIFace;

import edgeprocs.ReferenceData;

public class TabEncoderImpl implements ModelEncoderIFace {

    public static final String NAME = "TAB";

    @Override
    public String encode(MessageIFace m) throws Exception {
        return m.asDelimited(ReferenceData.DELIM_CHAR).toString();
    }

    @Override
    public MessageIFace decode(String s) throws Exception {

        String[] fields = s.split(ReferenceData.DELIM_CHAR);

        String className = BaseMessage.delimitedMessageType(ReferenceData.DELIM_CHAR, s);

        MessageIFace mi = (MessageIFace) Class.forName(ReferenceData.EDGEMESSAGES + className).newInstance();
        mi.setInternals(fields);

        return mi;

    }

    @Override
    public String getName() {
        return NAME;
    }


}
