package edgeprocs;

import java.util.Base64;
import java.util.HashMap;

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
import org.voltdb.types.TimestampType;
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
            "INSERT INTO device_messages(device_id, message_date, message_id, internal_message_id,status_code) "
            + "VALUES "
            + "(?,NOW,?,?,?);");

    public static final SQLStmt insertIntoStream0 = new SQLStmt(
            "INSERT INTO segment_0_stream(device_id, payload) VALUES (?,?);");

    public static final SQLStmt insertIntoStream1 = new SQLStmt(
            "INSERT INTO segment_1_stream(device_id, payload) VALUES (?,?);");

    public static final SQLStmt[] downstreamInserts = {insertIntoStream0, insertIntoStream1};

	// @formatter:on

    Gson g = new Gson();

    HashMap<String, ModelEncoderIFace> encoders = new HashMap<>();

    public VoltTable[] run(long deviceId, long callingOwner, String action, String serializedMessage)
            throws VoltAbortException {

        this.setAppStatusCode(ReferenceData.OK);

        // Deserialize message

        MessageIFace ourMessage;

        try {
            ourMessage = (MessageIFace) BaseMessage.fromJson(serializedMessage, g);

        } catch (JsonSyntaxException e) {
            reportError(ReferenceData.ERROR_BAD_JSON, deviceId, callingOwner, action, serializedMessage);
            return null;
        } catch (ClassNotFoundException e) {
            reportError(ReferenceData.ERROR_MESSAGE_OBJECT_CLASS_NOT_FOUND, deviceId, callingOwner, action,
                    serializedMessage);
            return null;
        }

        if (ourMessage.getExternallMessageId() <= 0) {
            reportError(ReferenceData.ERROR_MISSING_EXTERNAL_MESSAGE_ID, deviceId, callingOwner, action,
                    serializedMessage);
            return null;
        }

        if (ourMessage.getDeviceId() != deviceId) {
            reportError(ReferenceData.ERROR_DEVICE_ID_MISMATCH, deviceId, callingOwner, action, serializedMessage);
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
            reportError(ReferenceData.ERROR_UNKNOWN_UTIL_CO, deviceId, callingOwner, action, serializedMessage);
            return null;

        }

        if (!firstRound[1].advanceRow()) {

            reportError(ReferenceData.ERROR_UNKNOWN_DEVICE, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        VoltTable thisDeviceTable = firstRound[1];

        if (!firstRound[2].advanceRow()) {

            reportError(ReferenceData.ERROR_UNKNOWN_LOCATION, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        VoltTable thisLocationTable = firstRound[2];

        if (!firstRound[3].advanceRow()) {

            reportError(ReferenceData.ERROR_UNKNOWN_MODEL, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        VoltTable thisModelTable = firstRound[3];

        if (firstRound[4].advanceRow()) {

            reportError(ReferenceData.ERROR_DUPLICATE_MESSAGE, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        int destinationSegmentId = (int) thisLocationTable.getLong("segment_id");

        if (destinationSegmentId < 0 || destinationSegmentId >= downstreamInserts.length) {
            reportError(ReferenceData.ERROR_INVALID_SEGMENT_ID, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        String modelEncoderClassName = thisModelTable.getString("encoder_class_name");

        String modelNumber = thisDeviceTable.getString("model_number");
        long locationid = thisDeviceTable.getLong("location_id");
        long currentOwnerId = thisDeviceTable.getLong("current_owner_id");
        TimestampType lastFirmWareUpdate = thisDeviceTable.getTimestampAsTimestamp("last_firmware_update");

        if (callingOwner != currentOwnerId) {
            reportError(ReferenceData.ERROR_NOT_YOUR_DEVICE, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        // Do any needed translations
        ModelEncoderIFace ourEncoder = encoders.get(modelNumber);

        if (ourEncoder == null) {

            try {
                ourEncoder = (ModelEncoderIFace) Class.forName(modelEncoderClassName).newInstance();

            } catch (Exception e) {
                reportError(ReferenceData.ERROR_ENCODER_OBJECT_CLASS_NOT_FOUND, deviceId, callingOwner, action,
                        serializedMessage);
                return null;
            }

            encoders.put(modelNumber, ourEncoder);
        }

        String encodedMessage = null;

        try {
            encodedMessage = ourEncoder.encode(ourMessage);
            encodedMessage = Base64.getEncoder().encodeToString(encodedMessage.getBytes());
        } catch (Exception e) {
            reportError(ReferenceData.ERROR_ENCODER_FAILURE, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        final long thisTxId = this.getUniqueId();

        ourMessage.setInternalMessageId(thisTxId);
        ourMessage.setDestinationSegmentId(destinationSegmentId);
        String messageStatus = ReferenceData.MESSAGE_IN_FLIGHT;

        // Record messages existence...
        voltQueueSQL(createDeviceMessage, deviceId, ourMessage.getExternallMessageId(), thisTxId, messageStatus);

        // See if link has capacity

        // if so, send now...
        voltQueueSQL(downstreamInserts[ourMessage.getDestinationSegmentId()], deviceId, encodedMessage);

        // if not, buffer
        voltExecuteSQL();
        return null;
    }

    private void reportError(byte errorCode, long deviceId, long callingOwner, String action,
            String serializedMessage) {

        this.setAppStatusCode(errorCode);
        this.setAppStatusString(errorCode + ":" + deviceId + ":" + callingOwner + ":" + action + ":" + serializedMessage);
        voltExecuteSQL();

    }
}
