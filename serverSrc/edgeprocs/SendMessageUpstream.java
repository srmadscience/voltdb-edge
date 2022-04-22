package edgeprocs;

import java.util.Base64;
import java.util.HashMap;

import org.voltcore.logging.VoltLogger;

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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltse.edge.edgeencoders.ModelEncoderIFace;
import org.voltse.edge.edgemessages.MessageIFace;

import com.google.gson.Gson;

public class SendMessageUpstream extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getDevice = new SQLStmt(
            "SELECT * FROM devices WHERE device_id = ?");

    public static final SQLStmt getLocation = new SQLStmt(
            "SELECT l.* FROM locations l, devices d WHERE d.device_id = ? AND d.location_id = l.location_id;");

    public static final SQLStmt getModel = new SQLStmt(
            "SELECT m.* FROM models m, devices d WHERE d.device_id = ? AND d.model_number = m.model_number;");

    public static final SQLStmt getExistingMessage = new SQLStmt(
            "SELECT * FROM device_messages WHERE device_id = ? AND message_id = ?;");

    public static final SQLStmt updateMessageStatus = new SQLStmt(
            "UPDATE device_messages SET status_code = ? WHERE device_id = ? AND message_id = ?;");

    public static final SQLStmt insertIntoStream0 = new SQLStmt(
            "INSERT INTO powerco_0_stream(message_id, device_id, util_id, payload) VALUES (?,?,?,?);");

    public static final SQLStmt insertIntoStream1 = new SQLStmt(
            "INSERT INTO powerco_1_stream(message_id, device_id, util_id, payload) VALUES (?,?,?,?);");

    public static final SQLStmt[] upstreamInserts = {insertIntoStream0, insertIntoStream1};

    public static final SQLStmt reportError  = new SQLStmt("INSERT INTO error_stream "
            + "(message_id,device_id,error_code,event_kind,payload) "
            + "VALUES (?,?,?,?,?);");

    public static final SQLStmt createDeviceMessage = new SQLStmt(
            "INSERT INTO device_messages(device_id, message_date, message_id, internal_message_id,status_code,segment_id, current_owner_id) "
            + "VALUES "
            + "(?,NOW,?,?,?,?,?);");



	// @formatter:on

    Gson g = new Gson();

    HashMap<String, ModelEncoderIFace> encoders = new HashMap<>();

    static VoltLogger LOG = new VoltLogger("SendMessageUpstream");

    public VoltTable[] run(long deviceId, String serializedMessage) throws VoltAbortException {

        this.setAppStatusCode(ReferenceData.OK);

        // Sanity checks
        voltQueueSQL(getDevice, deviceId);
        voltQueueSQL(getLocation, deviceId);
        voltQueueSQL(getModel, deviceId);

        VoltTable[] firstRound = voltExecuteSQL();

        VoltTable thisDeviceTable = firstRound[0];
        if (!thisDeviceTable.advanceRow()) {

            reportError(this.getUniqueId(), ReferenceData.ERROR_UNKNOWN_DEVICE, deviceId, "SendMessageUpstream",
                    serializedMessage);
            return null;
        }

        VoltTable thisLocationTable = firstRound[1];
        if (!thisLocationTable.advanceRow()) {

            reportError(this.getUniqueId(), ReferenceData.ERROR_UNKNOWN_LOCATION, deviceId, "SendMessageUpstream",
                    serializedMessage);
            return null;
        }

        VoltTable thisModelTable = firstRound[2];

        if (!thisModelTable.advanceRow()) {

            reportError(this.getUniqueId(), ReferenceData.ERROR_UNKNOWN_MODEL, deviceId, "SendMessageUpstream",
                    serializedMessage);
            return null;
        }

        String modelEncoderClassName = thisModelTable.getString("encoder_class_name");

        String modelNumber = thisDeviceTable.getString("model_number");
        int currentOwnerId = (int) thisDeviceTable.getLong("current_owner_id");

        ModelEncoderIFace ourEncoder = getEncoder(deviceId, modelEncoderClassName, modelNumber);

        if (ourEncoder == null) {
            return null;
        }

        // Deserialize message
        MessageIFace ourMessage;

        String[] splitSerializedMessage = serializedMessage.split(",");

        if (splitSerializedMessage == null || splitSerializedMessage.length != 2) {
            reportError(this.getUniqueId(), ReferenceData.ERROR_DECODER_FAILURE, deviceId, "SendMessageUpstream",
                    modelEncoderClassName + ": message badly formatted,doesn't split: " + serializedMessage);
            return null;
        }

        String un64dMessage = null;

        try {
            un64dMessage = new String(Base64.getDecoder().decode(splitSerializedMessage[1]));
        } catch (Exception e) {
            reportError(this.getUniqueId(), ReferenceData.ERROR_DECODER_FAILURE, deviceId, "SendMessageUpstream",
                    modelEncoderClassName + ": message not Base 64: " + splitSerializedMessage[1]);
            return null;
        }

        try {
            ourMessage = ourEncoder.decode(un64dMessage);

        } catch (Exception e) {
            reportError(this.getUniqueId(), ReferenceData.ERROR_DECODER_FAILURE, deviceId, "SendMessageUpstream",
                    modelEncoderClassName + ":" + e.getMessage() + " " + serializedMessage);
            return null;
        }

        if (ourMessage.getDeviceId() != deviceId) {
            reportError(ourMessage.getExternallMessageId(), ReferenceData.ERROR_DEVICE_ID_MISMATCH, deviceId,
                    ourMessage.getMessageType(), serializedMessage);
            return null;
        }

        VoltTable existingMessage = null;

        if (ourMessage.isUpstreamOnly()) {

            ourMessage.setInternalMessageId(this.getUniqueId());

            voltQueueSQL(createDeviceMessage, deviceId, ourMessage.getExternallMessageId(),
                    ourMessage.getInternalMessageId(), ReferenceData.MESSAGE_IN_FLIGHT,
                    ourMessage.getDestinationSegmentId(), currentOwnerId);
            voltExecuteSQL();

        } else {

            if (ourMessage.getExternallMessageId() <= 0) {
                reportError(this.getUniqueId(), ReferenceData.ERROR_MISSING_EXTERNAL_MESSAGE_ID, deviceId,
                        ourMessage.getMessageType(), serializedMessage);
                return null;
            }

        }

        voltQueueSQL(getExistingMessage, deviceId, ourMessage.getExternallMessageId());

        existingMessage = voltExecuteSQL()[0];

        if (!existingMessage.advanceRow()) {

            reportError(this.getUniqueId(), ReferenceData.ERROR_BAD_EXTERNAL_MESSAGE_ID, deviceId,
                    ourMessage.getMessageType(), serializedMessage);
            return null;
        }

        if (ourMessage.getInternalMessageId() <= 0) {
            reportError(ourMessage.getExternallMessageId(), ReferenceData.ERROR_MISSING_INTERNAL_MESSAGE_ID, deviceId,
                    ourMessage.getMessageType(), serializedMessage);
            return null;
        }

        long internalMessageId = existingMessage.getLong("internal_message_id");
        String currentStatusCode = existingMessage.getString("status_code");

        if (internalMessageId != ourMessage.getInternalMessageId()) {

            reportError(ourMessage.getExternallMessageId(), ReferenceData.ERROR_BAD_INTERNAL_MESSAGE_ID, deviceId,
                    ourMessage.getMessageType(), serializedMessage);
            return null;
        }

        if (!currentStatusCode.equals(ReferenceData.MESSAGE_IN_FLIGHT)) {

            reportError(ourMessage.getExternallMessageId(), ReferenceData.ERROR_MESSAGE_NOT_IN_FLIGHT_IS_ACTIVE,
                    deviceId, ourMessage.getMessageType(), serializedMessage);
            return null;
        }

        ourMessage.setErrorMessage(ReferenceData.MESSAGE_DONE_STRING + "");
        voltQueueSQL(updateMessageStatus, ReferenceData.MESSAGE_DONE_STRING, deviceId,
                ourMessage.getExternallMessageId());

        String encodedMessage = Base64.getEncoder().encodeToString(ourMessage.asJson(g).getBytes());

        voltQueueSQL(upstreamInserts[currentOwnerId], ourMessage.getExternallMessageId(), deviceId, currentOwnerId,
                encodedMessage);

        voltExecuteSQL();
        return null;
    }

    private ModelEncoderIFace getEncoder(long deviceId, String modelEncoderClassName, String modelNumber) {
        // Do any needed translations
        ModelEncoderIFace ourEncoder = encoders.get(modelNumber);

        if (ourEncoder == null) {

            try {
                ourEncoder = (ModelEncoderIFace) Class.forName(modelEncoderClassName).newInstance();

            } catch (Exception e) {
                reportError(this.getUniqueId(), ReferenceData.ERROR_ENCODER_OBJECT_CLASS_NOT_FOUND, deviceId,
                        "SendMessageUpstream", e.getMessage());
                return null;
            }

            encoders.put(modelNumber, ourEncoder);
        }
        return ourEncoder;
    }

    private void reportError(long messageId, byte errorCode, long deviceId, String action, String payload) {

        voltQueueSQL(reportError, messageId, deviceId, errorCode, action, payload);

        this.setAppStatusCode(errorCode);
        this.setAppStatusString(errorCode + ":" + deviceId + ":" + action + ":" + payload);
        LOG.error(errorCode + ":" + messageId + ":" + deviceId + ":" + action + ":" + payload);
        voltExecuteSQL();

    }
}
