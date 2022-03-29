package org.voltdb.voltdbedge;

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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;

public class BaseIOTDemo {

//    public static final int START_AREA_ID = 94000;
//    public static final String GENERIC_QUERY_LICENCE_PLATE = "CAR430";
//    public static final long DEFAULT_WANTED_MINUTES = 10;
//    public static final long DEFAULT_WANTED_KWH = 75;
//    private static final long RANDOM_SEED = 42;
//
//    private static final String MAX_MINUTES_BEFORE_ARRIVAL = "MAX_MINUTES_BEFORE_ARRIVAL";
//    private static final String MINUTES_SPENT_CHARGING = "MINUTES_SPENT_CHARGING";
//    private static final String MAX_OVERAGE_TIME = "MAX_OVERAGE_TIME";

    private static final int LOCATION_COUNT = 16;
    private static final int OWNER_COUNT = 2;
    private static final long RANDOM_SEED = 0;

    public static String[] DELETABLE_TABLES = { "devices" };

    /**
     * Connect to VoltDB using a comma delimited hostname list.
     *
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    protected static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    protected static void createDevices(Client mainClient, int howMany, int tpMs)
            throws InterruptedException, IOException, NoConnectionsException {

        Random r = new Random(RANDOM_SEED);

        TransactionSpeedRegulator tsm = new TransactionSpeedRegulator(tpMs, TransactionSpeedRegulator.NO_END_DATE);

        msg("Creating " + howMany + " devices");

        for (int i = 0; i <= howMany; i++) {

            try {

                tsm.waitIfNeeded();

                NullCallback ncb = new NullCallback();

                mainClient.callProcedure(ncb, "ProvisionDevice", i, "Model" + (i % 3), r.nextInt(LOCATION_COUNT),
                        r.nextInt(OWNER_COUNT));

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        mainClient.drain();

    }

    protected static void deleteOldData(Client mainClient)
            throws InterruptedException, IOException, NoConnectionsException {

        msg("Deleting and replacing old data");
        try {

            for (String element : DELETABLE_TABLES) {

                msg("DELETE FROM " + element + ";");
                mainClient.callProcedure("@AdHoc", "DELETE FROM " + element + ";");

            }

            mainClient.drain();

        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }

    }

//    @SuppressWarnings("unused")
//    protected static void runSingleVehicleExample(Client mainClient)
//            throws InterruptedException, IOException, NoConnectionsException, ProcCallException {
//
//        Random r = new Random(RANDOM_SEED);
//        final Date targetTime = new Date(System.currentTimeMillis() + (60 * 1000));
//
//        msg("Vehicle " + GENERIC_QUERY_LICENCE_PLATE + " wants to park in area " + START_AREA_ID);
//        msg("Check options for " + targetTime + " ...");
//        ClientResponse availableSpacesQuery = mainClient.callProcedure("GetOptions", START_AREA_ID, targetTime);
//
//        VoltTable availableSpacesTable = availableSpacesQuery.getResults()[0];
//
//        msg(System.lineSeparator() + availableSpacesTable.toFormattedString());
//
//        availableSpacesTable.resetRowPosition();
//
//        if (availableSpacesTable.advanceRow()) {
//
//            long myChargerId = availableSpacesTable.getLong("CHARGER_ID");
//            BigDecimal myRate = availableSpacesTable.getDecimalAsBigDecimal("KWH_RATE");
//
//            msg("Reserving charger " + myChargerId + "...");
//
//            ClientResponse reservationRequest = mainClient.callProcedure("SelectOption", START_AREA_ID, targetTime,
//                    myChargerId, DEFAULT_WANTED_MINUTES, DEFAULT_WANTED_KWH, GENERIC_QUERY_LICENCE_PLATE);
//
//            for (int i = 0; i < reservationRequest.getResults().length; i++) {
//                msg(System.lineSeparator() + reservationRequest.getResults()[i].toFormattedString());
//            }
//
//            if (reservationRequest.getAppStatus() != ReferenceData.STATUS_CHARGER_FOUND) {
//                msg(reservationRequest.getAppStatusString());
//            }
//
//            if (reservationRequest.getResults().length == 0) {
//                msg("Reservation failed - existing");
//            } else {
//                VoltTable requestRecordTable = reservationRequest.getResults()[reservationRequest.getResults().length
//                        - 1];
//                requestRecordTable.advanceRow();
//                long requestId = requestRecordTable.getLong("REQUEST_ID");
//                msg("Reservation succeeded - id  :" + requestId);
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//                msg("Waiting 1 minute to see if wait fees appear...");
//                Thread.sleep(60001);
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//                msg("Start Charging...");
//                ClientResponse chargeRequest = mainClient.callProcedure("start_charge_session", START_AREA_ID,
//                        myChargerId, START_AREA_ID, requestId);
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//                final double kwh = r.nextInt(1000000) / 1000;
//                final double amount = Math.round(kwh * myRate.doubleValue());
//
//                Thread.sleep(6000);
//                msg("Report Usage of " + kwh + "kwh for US$" + amount + "...");
//
//                ClientResponse usageRequest =
//
//                        mainClient.callProcedure("charge_kwh_fees", kwh, amount, requestId, START_AREA_ID, myChargerId,
//                                kwh, amount, START_AREA_ID, myChargerId);
//
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//                msg("Stop Charging, but remain at the charger...");
//                ClientResponse stopRequest = mainClient.callProcedure("stop_charge_session", START_AREA_ID, myChargerId,
//                        START_AREA_ID, requestId);
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//                msg("Waiting 1 minute to see if wait fees appear...");
//                Thread.sleep(60001);
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//                msg("End Charge Session...");
//                ClientResponse endChargeRequest = mainClient.callProcedure("end_charge_session", START_AREA_ID,
//                        myChargerId, START_AREA_ID, requestId);
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//            }
//
//        } else {
//            msg("No space available - exiting");
//        }
//
//    }
//
//    protected static long runManyVehicleBenchmark(int userCount, int areaCount, int tpMs, int durationSeconds,
//            int globalQueryFreqSeconds, Client mainClient, Client otherClient)
//            throws InterruptedException, IOException, NoConnectionsException, ProcCallException {
//
//        Random r = new Random(RANDOM_SEED);
//
//        HashMap<String, Long> parameters = getParameters(mainClient);
//
//        VehicleState users = new VehicleState(userCount, r);
//
//        final long startMsRun = System.currentTimeMillis();
//        long currentMs = System.currentTimeMillis();
//        int tpThisMs = 0;
//
//        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000);
//
//        // How many transactions we've done...
//        long tranCount = 0;
//        long inFlightCount = 0;
//        long isActiveCount = 0;
//        long stateWaitingCount = 0;
//        long stateChargingCount = 0;
//        long stateAskingCount = 0;
//        long stateBookingCount = 0;
//        long stateLookingCount = 0;
//        long lastGlobalQueryMs = System.currentTimeMillis();
//
//        msg("starting...");
//
//        long oldSessionsClosed = forceClose( userCount,  tpMs,  users,  mainClient,  r );
//
//        if (oldSessionsClosed > 0) {
//            msg("Closed " + oldSessionsClosed + " old sessions");
//        }
//
//        while (endtimeMs > System.currentTimeMillis()) {
//
//            if (tpThisMs++ > tpMs) {
//
//                while (currentMs == System.currentTimeMillis()) {
//                    Thread.sleep(0, 50000);
//
//                }
//
//                currentMs = System.currentTimeMillis();
//                tpThisMs = 0;
//            }
//
//            int randomuser = r.nextInt(userCount);
//
//            if (users.isTxInFlight(randomuser)) {
//                inFlightCount++;
//            } else if (users.isActive(randomuser)) {
//                isActiveCount++;
//            } else {
//
//                users.startTran(randomuser);
//
//                if (users.getState(randomuser) == VehicleState.STATUS_ELSEWHERE) {
//
//                    int randomArea = r.nextInt(areaCount) + START_AREA_ID;
//
//                    int maxMinutesBeforeArrival = parameters.getOrDefault(MAX_MINUTES_BEFORE_ARRIVAL, 1l).intValue();
//                    maxMinutesBeforeArrival = r.nextInt(maxMinutesBeforeArrival) + 1;
//                    final Date targetTime = new Date(
//                            System.currentTimeMillis() + (maxMinutesBeforeArrival * 60 * 1000));
//                    stateWaitingCount++;
//
//                    FindParkingCallback fbcp = new FindParkingCallback(users, randomuser, randomArea, r, otherClient,targetTime);
//
//                    mainClient.callProcedure(fbcp, "GetOptions", randomArea, targetTime);
//
//                } else if (users.getState(randomuser) == VehicleState.STATUS_WAITING) {
//
//                    NullCallback ncb = new NullCallback();
//
//                    Long minutesSpentCharging = parameters.getOrDefault(MINUTES_SPENT_CHARGING, 2l);
//
//                    Date endTime = new Date(System.currentTimeMillis() + (minutesSpentCharging * 60 * 1000));
//                    users.startCharging(randomuser, endTime);
//
//                    long chargerid = users.getChargerId(randomuser);
//                    long areaId = users.getAreaId(randomuser);
//
//                    stateWaitingCount++;
//
//                    mainClient.callProcedure(ncb, "start_charge_session", areaId, chargerid, areaId,
//                            users.getRequestId(randomuser));
//
//                } else if (users.getState(randomuser) == VehicleState.STATUS_CHARGING) {
//
//                    long maxOverageTime = parameters.getOrDefault(MAX_OVERAGE_TIME, 2l);
//
//                    finishCharging(mainClient, r, users, randomuser, r.nextInt((int) maxOverageTime));
//
//                } else if (users.getState(randomuser) == VehicleState.STATUS_STOPPED) {
//
//                    finishCharging(mainClient, r, users, randomuser, 0);
//
//                }
//
//            }
//
//            if (tranCount++ % 100000 == 0) {
//                msg("On transaction #" + tranCount);
//            }
//
//            // See if we need to do global queries...
//            if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000) < System.currentTimeMillis()) {
//
//                lastGlobalQueryMs = System.currentTimeMillis();
//
//                queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//                parameters = getParameters(mainClient);
//
//                msg("Skipped because transaction was in flight = " + inFlightCount);
//                msg("Skipped because vehicle was waiting = " + isActiveCount);
//                msg("Unable to find parking = " + users.getUnableToFindParkingCounter());
//                msg("Space stolen when trying to book = " + users.getSpaceStolenParkingCounter());
//
//                msg("Charging=" + stateChargingCount);
//                msg("Asking=" + stateAskingCount);
//                msg("Booking=" + stateBookingCount);
//                msg("Waiting=" + stateWaitingCount);
//                msg("Looking=" + stateLookingCount);
//
//            }
//
//        }
//
//        msg("ending parking sessions");
//
//        long forceClosed = forceClose( userCount,  tpMs,  users,  mainClient,  r );
//
//        msg("finished closing sessions ");
//        queryUserAndStats(mainClient, GENERIC_QUERY_LICENCE_PLATE);
//
//        mainClient.drain();
//        msg("Queue drained");
//
//        long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
//        msg("Processed " + tranCount + " transactions in " + elapsedTimeMs + " milliseconds");
//
//        double tps = tranCount;
//        tps = tps / elapsedTimeMs;
//        tps = tps * 1000;
//
//        msg("TPS = " + tps);
//
//        msg("Skipped because transaction was in flight = " + inFlightCount);
//        msg("Skipped because vehicle was waiting = " + isActiveCount);
//        msg("Unable to find parking = " + users.getUnableToFindParkingCounter());
//        msg("Space stolen when trying to book = " + users.getSpaceStolenParkingCounter());
//
//        msg("Charging=" + stateChargingCount);
//        msg("Asking=" + stateAskingCount);
//        msg("Booking=" + stateBookingCount);
//        msg("Looking=" + stateLookingCount);
//        msg("ForceClosed=" + forceClosed);
//
//        return (long) tps;
//    }
//
//    private static HashMap<String, Long> getParameters(Client mainClient) {
//
//        HashMap<String, Long> parameters = new HashMap<String, Long>();
//
//        try {
//            ClientResponse paramResponse = mainClient.callProcedure("GetParams");
//
//            while (paramResponse.getResults()[0].advanceRow()) {
//                String key = paramResponse.getResults()[0].getString("param_name");
//                Long value = paramResponse.getResults()[0].getLong("param_value");
//                msg("Param " + key + " = " + value);
//                parameters.put(key, value);
//            }
//
//        } catch (IOException | ProcCallException e) {
//            e.printStackTrace();
//        }
//
//        return parameters;
//    }
//
//    private static long forceClose(int userCount, int tpMs, VehicleState users, Client mainClient, Random r ) throws NoConnectionsException, IOException {
//
//        int tpThisMs = 0;
//        long currentMs = System.currentTimeMillis();
//
//        msg("ending existing parking sessions");
//
//        long forceClosed = 0;
//
//        for (int i = 0; i < userCount; i++) {
//
//            if (tpThisMs++ > tpMs) {
//
//                while (currentMs == System.currentTimeMillis()) {
//                    try {
//                        Thread.sleep(0, 50000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//
//                }
//
//                currentMs = System.currentTimeMillis();
//                tpThisMs = 0;
//            }
//
//            if (users.getState(i) != VehicleState.STATUS_ELSEWHERE) {
//
//                finishCharging(mainClient, r, users, i, 0);
//                forceClosed++;
//
//            }
//        }
//
//        return forceClosed;
//
//    }
//
//    private static void finishCharging(Client mainClient, Random r, VehicleState users, int randomuser,
//            int overageTimeMinutes) throws IOException, NoConnectionsException {
//        NullCallback ncb = new NullCallback();
//
//        long qty = 1;
//        double amount = 1 + r.nextInt(3);
//
//        long chargerid = users.getChargerId(randomuser);
//        long areaId = users.getAreaId(randomuser);
//        long requestId = users.getRequestId(randomuser);
//
//        if (overageTimeMinutes > 2) {
//
//            users.endChargingWithOverage(randomuser, overageTimeMinutes);
//            mainClient.callProcedure(ncb, "stop_charge_session", areaId, users.getChargerId(randomuser), areaId,
//                    users.getRequestId(randomuser));
//
//        } else {
//
//            users.endCharging(randomuser);
//            mainClient.callProcedure(ncb, "charge_kwh_fees", qty, amount, requestId, areaId, chargerid, qty, amount,
//                    areaId, chargerid);
//            mainClient.callProcedure(ncb, "end_charge_session", areaId, users.getChargerId(randomuser), areaId,
//                    users.getRequestId(randomuser));
//        }
//
//    }
//
//    protected static void queryUserAndStats(Client mainClient, String plateId)
//            throws IOException, NoConnectionsException, ProcCallException {
//
//        // Query user #queryUserId...
//        msg("Query vehicle #" + plateId + " requests and charges...");
//        ClientResponse userResponse = mainClient.callProcedure("get_vehicle", plateId, plateId);
//
//        msg("Requests:" + System.lineSeparator() + userResponse.getResults()[0].toFormattedString());
//        msg("Charges:" + System.lineSeparator() + userResponse.getResults()[1].toFormattedString());
//
//        ClientResponse promBLResponse = mainClient.callProcedure("ShowActivity__promBL");
//
//        for (int i = 0; i < promBLResponse.getResults().length; i++) {
//            msg("promBL Line :" + i + " " + System.lineSeparator()
//                    + promBLResponse.getResults()[i].toFormattedString());
//        }
//
//    }

    public static void main(String[] args) {

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 5) {
            msg("Usage: hostnames  tpms durationseconds usercount  queryseconds");
            System.exit(1);
        }

        String hostnames = args[0];
        int tps = Integer.parseInt(args[1]);
        int duration = Integer.parseInt(args[2]);
        int howmany = Integer.parseInt(args[3]);
        int queryseconds = Integer.parseInt(args[4]);

        msg("TPS=" + tps + ", durationSeconds=" + duration + ", query interval seconds = " + queryseconds + ", howmany="
                + howmany);

        try {
            Client c = connectVoltDB(hostnames);

            deleteOldData(c);
            createDevices(c, howmany, tps);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

}
