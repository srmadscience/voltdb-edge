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
import org.voltse.edge.edgemessages.BaseMessage;
import org.voltse.edge.edgemessages.MessageIFace;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class SendMessageDownstream extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getUtil = new SQLStmt(
            "SELECT * FROM utilities WHERE util_id = ?;");

    public static final SQLStmt getDevice = new SQLStmt(
            "SELECT * FROM devices WHERE device_id = ?");

    public static final SQLStmt getLocation = new SQLStmt(
            "SELECT l.* FROM locations l, devices d WHERE d.device_id = ? AND d.location_id = l.location_id;");

    public static final SQLStmt getModel = new SQLStmt(
            "SELECT m.* FROM models m, devices d WHERE d.device_id = ? AND d.model_number = m.model_number;");

    public static final SQLStmt getExistingMessage = new SQLStmt(
            "SELECT * FROM device_messages WHERE device_id = ? AND message_id = ?;");

    public static final SQLStmt createDeviceMessage = new SQLStmt(
            "INSERT INTO device_messages(device_id, message_date, message_id, internal_message_id"
            + ",status_code,segment_id,current_owner_id) "
            + "VALUES "
            + "(?,NOW,?,?,?,?,?);");

     public static final SQLStmt reportError  = new SQLStmt("INSERT INTO error_stream "
            + "(message_id,device_id,error_code,event_kind,payload) "
            + "VALUES (?,?,?,?,?);");


    public static final SQLStmt insertIntoStream0 = new SQLStmt(
            "INSERT INTO segment_0_stream(message_id, device_id, payload) VALUES (?,?,?);");

    public static final SQLStmt insertIntoStream1 = new SQLStmt(
            "INSERT INTO segment_1_stream(message_id, device_id, payload) VALUES (?,?,?);");
    
    public static final SQLStmt insertIntoStream2 = new SQLStmt(
            "INSERT INTO segment_2_stream(message_id, device_id, payload) VALUES (?,?,?);");

 
    public static final SQLStmt insertIntoStream3 = new SQLStmt(
            "INSERT INTO segment_3_stream(message_id, device_id, payload) VALUES (?,?,?);");

 
    public static final SQLStmt insertIntoStream4 = new SQLStmt(
            "INSERT INTO segment_4_stream(message_id, device_id, payload) VALUES (?,?,?);");

 
    public static final SQLStmt insertIntoStream5= new SQLStmt(
            "INSERT INTO segment_5_stream(message_id, device_id, payload) VALUES (?,?,?);");
 
    public static final SQLStmt insertIntoStream6 = new SQLStmt(
            "INSERT INTO segment_6_stream(message_id, device_id, payload) VALUES (?,?,?);");

 
    public static final SQLStmt insertIntoStream7 = new SQLStmt(
            "INSERT INTO segment_7_stream(message_id, device_id, payload) VALUES (?,?,?);");

 
    public static final SQLStmt insertIntoStream8 = new SQLStmt(
            "INSERT INTO segment_8_stream(message_id, device_id, payload) VALUES (?,?,?);");

    public static final SQLStmt insertIntoStream9 = new SQLStmt(
            "INSERT INTO segment_9_stream(message_id, device_id, payload) VALUES (?,?,?);");

    public static final SQLStmt[] downstreamInserts = {insertIntoStream0, insertIntoStream1
            , insertIntoStream2,insertIntoStream3, insertIntoStream4, insertIntoStream5
            , insertIntoStream6, insertIntoStream7, insertIntoStream8, insertIntoStream9};

    
    // @formatter:on

    Gson g = new Gson();

    HashMap<String, ModelEncoderIFace> encoders = new HashMap<>();

    static VoltLogger LOG = new VoltLogger("SendMessageDownstream");

    public VoltTable[] run(long deviceId, long callingOwner, String serializedMessage) throws VoltAbortException {

        this.setAppStatusCode(ReferenceData.OK);

        final long thisTxId = this.getUniqueId();

        this.setAppStatusCode(ReferenceData.OK);

        // Deserialize message

        MessageIFace ourMessage;

        String un64dMessage = null;

        try {
            un64dMessage = new String(Base64.getDecoder().decode(serializedMessage));
        } catch (Exception e) {
            reportError(this.getUniqueId(), ReferenceData.ERROR_DECODER_FAILURE, deviceId, "SendMessageUpstream",
                    "message not Base 64: " + serializedMessage);
            return null;
        }

        try {

            ourMessage = (MessageIFace) BaseMessage.fromJson(un64dMessage, g);

        } catch (JsonSyntaxException e) {
            reportError(thisTxId, ReferenceData.ERROR_BAD_JSON, deviceId, "SendMessageDownstream", serializedMessage);
            return null;
        } catch (ClassNotFoundException e) {
            reportError(thisTxId, ReferenceData.ERROR_MESSAGE_OBJECT_CLASS_NOT_FOUND, deviceId, "SendMessageDownstream",
                    serializedMessage);
            return null;
        }

        if (ourMessage.isUpstreamOnly()) {
            reportError(thisTxId, ReferenceData.ERROR_MESSAGE_CANT_BE_SENT_DOWNSTREAM, deviceId,
                    ourMessage.getMessageType(), un64dMessage);
            return null;
        }

        if (ourMessage.getExternallMessageId() <= 0) {
            reportError(thisTxId, ReferenceData.ERROR_MISSING_EXTERNAL_MESSAGE_ID, deviceId,
                    ourMessage.getMessageType(), un64dMessage);
            return null;
        }

        if (ourMessage.getDeviceId() != deviceId) {
            reportError(thisTxId, ReferenceData.ERROR_DEVICE_ID_MISMATCH, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        // Sanity checks
        voltQueueSQL(getUtil, callingOwner);
        voltQueueSQL(getDevice, deviceId);
        voltQueueSQL(getLocation, deviceId);
        voltQueueSQL(getModel, deviceId);
        voltQueueSQL(getExistingMessage, deviceId, ourMessage.getExternallMessageId());

        VoltTable[] firstRound = voltExecuteSQL();

        if (!firstRound[0].advanceRow()) {
            reportError(thisTxId, ReferenceData.ERROR_UNKNOWN_UTIL_CO, deviceId, ourMessage.getMessageType(),
                    callingOwner + " " + un64dMessage);
            return null;

        }

        if (!firstRound[1].advanceRow()) {

            reportError(thisTxId, ReferenceData.ERROR_UNKNOWN_DEVICE, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        VoltTable thisDeviceTable = firstRound[1];

        if (!firstRound[2].advanceRow()) {

            reportError(thisTxId, ReferenceData.ERROR_UNKNOWN_LOCATION, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        VoltTable thisLocationTable = firstRound[2];

        if (!firstRound[3].advanceRow()) {

            reportError(thisTxId, ReferenceData.ERROR_UNKNOWN_MODEL, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        VoltTable thisModelTable = firstRound[3];

        if (firstRound[4].advanceRow()) {

            reportError(thisTxId, ReferenceData.ERROR_DUPLICATE_MESSAGE, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        int destinationSegmentId = (int) thisLocationTable.getLong("segment_id");

        if (destinationSegmentId < 0 || destinationSegmentId >= downstreamInserts.length) {
            reportError(thisTxId, ReferenceData.ERROR_INVALID_SEGMENT_ID, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        String modelEncoderClassName = thisModelTable.getString("encoder_class_name");

        String modelNumber = thisDeviceTable.getString("model_number");
        long currentOwnerId = thisDeviceTable.getLong("current_owner_id");

        if (callingOwner != currentOwnerId) {
            reportError(thisTxId, ReferenceData.ERROR_NOT_YOUR_DEVICE, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        // Do any needed translations
        ModelEncoderIFace ourEncoder = encoders.get(modelNumber);

        if (ourEncoder == null) {

            try {
                ourEncoder = (ModelEncoderIFace) Class.forName(modelEncoderClassName).newInstance();

            } catch (Exception e) {
                reportError(thisTxId, ReferenceData.ERROR_ENCODER_OBJECT_CLASS_NOT_FOUND, deviceId,
                        ourMessage.getMessageType(), un64dMessage);
                return null;
            }

            encoders.put(modelNumber, ourEncoder);
        }

        ourMessage.setInternalMessageId(thisTxId);
        ourMessage.setDestinationSegmentId(destinationSegmentId);
        ourMessage.setCreateDate(getTransactionTime());
        ourMessage.setCallingOwner(callingOwner);
        String messageStatus = ReferenceData.MESSAGE_IN_FLIGHT;

        String encodedMessage = null;

        try {
            encodedMessage = ourEncoder.encode(ourMessage);
            encodedMessage = Base64.getEncoder().encodeToString(encodedMessage.getBytes());
        } catch (Exception e) {
            reportError(thisTxId, ReferenceData.ERROR_ENCODER_FAILURE, deviceId, ourMessage.getMessageType(),
                    un64dMessage);
            return null;
        }

        // Record messages existence...
        voltQueueSQL(createDeviceMessage, deviceId, ourMessage.getExternallMessageId(), thisTxId, messageStatus,
                ourMessage.getDestinationSegmentId(), currentOwnerId);

        // See if link has capacity

        // if so, send now...
        voltQueueSQL(downstreamInserts[ourMessage.getDestinationSegmentId()], ourMessage.getExternallMessageId(),
                deviceId, encodedMessage);

        // if not, buffer
        voltExecuteSQL();
        return null;
    }

    private void reportError(long messageId, byte errorCode, long deviceId, String action, String payload) {

        voltQueueSQL(reportError, messageId, deviceId, errorCode, action, payload);

        this.setAppStatusCode(errorCode);
        this.setAppStatusString(errorCode + ":" + deviceId + ":" + action + ":" + payload);

        LOG.error(deviceId + ":" + action + ":" + payload);
        voltExecuteSQL();

    }
}
