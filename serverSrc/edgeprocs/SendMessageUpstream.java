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
import org.voltse.edge.edgemessages.MessageIFace;

import com.google.gson.Gson;

public class SendMessageUpstream extends VoltProcedure {

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

    public static final SQLStmt updateMessageStatus = new SQLStmt(
            "UPDATE device_messages SET status_code = ? WHERE device_id = ? AND message_id = ?;");

    public static final SQLStmt insertIntoStream0 = new SQLStmt(
            "INSERT INTO powerco_0_stream(device_id, util_id, payload) VALUES (?,?,?);");

    public static final SQLStmt insertIntoStream1 = new SQLStmt(
            "INSERT INTO powerco_1_stream(device_id, util_id, payload) VALUES (?,?,?);");

    public static final SQLStmt[] upstreamInserts = {insertIntoStream0, insertIntoStream1};

	// @formatter:on

    Gson g = new Gson();

    HashMap<String, ModelEncoderIFace> encoders = new HashMap<>();

    public VoltTable[] run(long deviceId, long callingOwner, String action, String serializedMessage)
            throws VoltAbortException {

        this.setAppStatusCode(ReferenceData.OK);

        // Sanity checks
        voltQueueSQL(getUtil, callingOwner);
        voltQueueSQL(getDevice, deviceId);
        voltQueueSQL(getLocation, deviceId);
        voltQueueSQL(getModel, deviceId);

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

        String modelEncoderClassName = thisModelTable.getString("encoder_class_name");

        String modelNumber = thisDeviceTable.getString("model_number");
        long locationid = thisDeviceTable.getLong("location_id");
        int currentOwnerId = (int) thisDeviceTable.getLong("current_owner_id");
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

        // Deserialize message

        MessageIFace ourMessage;

        try {
            ourMessage = ourEncoder.decode(serializedMessage);

        } catch (Exception e) {
            reportError(ReferenceData.ERROR_DECODER_FAILURE, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        if (ourMessage.getExternallMessageId() <= 0) {
            reportError(ReferenceData.ERROR_MISSING_EXTERNAL_MESSAGE_ID, deviceId, callingOwner, action,
                    serializedMessage);
            return null;
        }

        if (ourMessage.getInternalMessageId() <= 0) {
            reportError(ReferenceData.ERROR_MISSING_INTERNAL_MESSAGE_ID, deviceId, callingOwner, action,
                    serializedMessage);
            return null;
        }

        if (ourMessage.getDeviceId() != deviceId) {
            reportError(ReferenceData.ERROR_DEVICE_ID_MISMATCH, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        voltQueueSQL(getExistingMessage, deviceId, ourMessage.getExternallMessageId());

        VoltTable[] secondRound = voltExecuteSQL();

        VoltTable existingMessage = secondRound[0];

        if (!existingMessage.advanceRow()) {

            reportError(ReferenceData.ERROR_BAD_EXTERNAL_MESSAGE_ID, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        long internalMessageId = existingMessage.getLong("internal_message_id");
        String currentStatusCode = existingMessage.getString("status_code");

        if (internalMessageId != ourMessage.getInternalMessageId()) {

            reportError(ReferenceData.ERROR_BAD_INTERNAL_MESSAGE_ID, deviceId, callingOwner, action, serializedMessage);
            return null;
        }

        if (!currentStatusCode.equals(ReferenceData.MESSAGE_IN_FLIGHT)) {

            reportError(ReferenceData.ERROR_MESSAGE_NOT_IN_FLIGHT_IS_ACTIVE, deviceId, callingOwner, action,
                    serializedMessage);
            return null;
        }

        voltQueueSQL(updateMessageStatus, ReferenceData.MESSAGE_DONE, deviceId, ourMessage.getExternallMessageId());

        String encodedMessage = Base64.getEncoder().encodeToString(ourMessage.asJson(g).getBytes());
        voltQueueSQL(upstreamInserts[currentOwnerId], deviceId, currentOwnerId, encodedMessage);

        voltExecuteSQL();
        return null;
    }

    private void reportError(byte errorCode, long deviceId, long callingOwner, String action,
            String serializedMessage) {

        this.setAppStatusCode(errorCode);
        this.setAppStatusString(deviceId + ":" + callingOwner + ":" + action + ":" + serializedMessage);
        voltExecuteSQL();

    }
}
