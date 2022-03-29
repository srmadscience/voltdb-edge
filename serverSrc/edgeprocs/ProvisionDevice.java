package edgeprocs;

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

public class ProvisionDevice extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getLocation = new SQLStmt(
            "SELECT * FROM locations WHERE location_id = ?;");

    public static final SQLStmt getUtil = new SQLStmt(
            "SELECT * FROM utilities WHERE util_id = ?;");

    public static final SQLStmt getModel = new SQLStmt(
            "SELECT m.* FROM models m WHERE  m.model_number = ?;");

    public static final SQLStmt getDevice = new SQLStmt(
            "SELECT * FROM devices WHERE device_id = ?");

    public static final SQLStmt createDevice = new SQLStmt(
            "INSERT INTO devices(device_id, model_number, location_id, current_owner_id, last_firmware_update) "
            + "VALUES "
            + "(?,?,?,?,NOW);");

	// @formatter:on

    public VoltTable[] run(long deviceId, String modelNumber, long locationId, long owner) throws VoltAbortException {

        this.setAppStatusCode(ReferenceData.OK);

        // See if we know about this area and charger...
        voltQueueSQL(getLocation, locationId);
        voltQueueSQL(getUtil, owner);
        voltQueueSQL(getModel, modelNumber);
        voltQueueSQL(getDevice, deviceId);

        VoltTable[] firstRound = voltExecuteSQL();

        if (!firstRound[0].advanceRow()) {
            this.setAppStatusCode(ReferenceData.ERROR_UNKNOWN_LOCATION);
            this.setAppStatusString("Location not known");
            return firstRound;
        }

        if (!firstRound[1].advanceRow()) {
            this.setAppStatusCode(ReferenceData.ERROR_UNKNOWN_UTIL_CO);
            this.setAppStatusString("Utility co not known");
            return firstRound;
        }

        if (! firstRound[2].advanceRow()) {
            this.setAppStatusCode(ReferenceData.ERROR_UNKNOWN_MODEL);
            this.setAppStatusString("Model not known");
            return firstRound;
        }

        if (firstRound[3].advanceRow()) {
            this.setAppStatusCode(ReferenceData.DEVICE_ALREADY_EXISTS);
            this.setAppStatusString("Device Already Exists");
            return firstRound;
        }

        voltQueueSQL(createDevice, deviceId, modelNumber, locationId, owner);

        return voltExecuteSQL(true);
    }
}
