package edgeprocs;

import org.voltse.edge.edgeencoders.JsonEncoderImpl;
import org.voltse.edge.edgeencoders.TabEncoderImpl;
import org.voltse.edge.edgemessages.MessageIFace;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

/**
 * Possible response codes.
 *
 */
public class ReferenceData {

    public final static String TEST_JSON_METER_NAME = "MeterTron100";
    public final static String TEST_DELIM_METER_NAME = "HomeMeter100";

    public static final String[] METER_TYPES = { TEST_JSON_METER_NAME, TEST_DELIM_METER_NAME };

    public static final String EDGEMESSAGES = "org.voltse.edge.edgemessages.";
    public static final String EDGEENCODERS = "org.voltse.edge.edgeencoders.";
    public static final String MESSAGE_IN_FLIGHT = "MIF";
    public static final String MESSAGE_BUFFERED_LINK = "BL";

    public static final String DELIM_CHAR = "\t";

    public static final byte OK = 0;
    public static final byte ERROR_UNKNOWN_UTIL_CO = 1;
    public static final byte ERROR_UNKNOWN_DEVICE = 2;
    public static final byte ERROR_NOT_YOUR_DEVICE = 3;
    public static final byte ERROR_UNKNOWN_LOCATION = 4;
    public static final byte ERROR_UNKNOWN_MODEL = 5;
    public static final byte ERROR_MODEL_CLASS_UNUSABLE = 6;
    public static final byte ERROR_BAD_JSON = 7;
    public static final byte ERROR_ENCODER_OBJECT_CLASS_NOT_FOUND = 8;
    public static final byte ERROR_MESSAGE_OBJECT_CLASS_NOT_FOUND = 9;
    public static final byte ERROR_DUPLICATE_MESSAGE = 10;
    public static final byte DEVICE_ALREADY_EXISTS = 11;
    public static final byte ERROR_BAD_INTERNAL_MESSAGE_ID = 12;
    public static final byte ERROR_BAD_EXTERNAL_MESSAGE_ID = 18;
    public static final byte ERROR_MISSING_EXTERNAL_MESSAGE_ID = 13;
    public static final byte ERROR_DEVICE_ID_MISMATCH = 14;
    public static final byte ERROR_ENCODER_FAILURE = 15;
    public static final byte ERROR_DECODER_FAILURE = 16;
    public static final byte ERROR_MISSING_INTERNAL_MESSAGE_ID = 17;
    public static final byte ERROR_MESSAGE_NOT_IN_FLIGHT_IS_ACTIVE = 19;
    public static final byte MESSAGE_DONE = 20;
    public static final String MESSAGE_DONE_STRING = "DONE";
    public static final byte ERROR_INVALID_SEGMENT_ID = 21;
    public static final byte ERROR_MESSAGE_CANT_BE_SENT_DOWNSTREAM = 22;

    public static final String SEGMENT_1_TOPIC = "segment_1_topic";
    public static final String UPSTREAM_TOPIC = "upstream_1_topic";
    public static final String DOWNSTREAM_TOPIC = "downstream_1_topic";
    public static final String POWERCO_1_TOPIC = "powerco_1_topic";

    public static String getdeviceEncoding(MessageIFace theMessage) {

        return getdeviceEncoding(theMessage.getDeviceId());
    }

    public static String getdeviceEncoding(long deviceId) {

        if (deviceId % 2 == 0) {

            return TabEncoderImpl.NAME;
        }

        return JsonEncoderImpl.NAME;
    }

    public static String getdeviceEncoderClassName(long deviceId) {

        if (deviceId % 2 == 0) {

            return "org.voltse.edge.edgeencoders.TabEncoderImpl";
        }

        return "org.voltse.edge.edgeencoders.JsonEncoderImpl";
    }

    public static String getdeviceEncoderClassName(MessageIFace theMessage) {

        return getdeviceEncoderClassName(theMessage.getDeviceId());
    }

}
