/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

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
