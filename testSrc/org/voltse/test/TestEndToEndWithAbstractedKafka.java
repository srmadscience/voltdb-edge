package org.voltse.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltdbedge.DeviceEmulator;
import org.voltdb.voltdbedge.PowerCoEmulator;
import org.voltse.edge.edgemessages.EnableFeatureMessage;

import edgeprocs.ReferenceData;

class TestEndToEndWithAbstractedKafka {



    final long startMs = System.currentTimeMillis();


    PowerCoEmulator p ;
    DeviceEmulator d ;
    Client c;

    String[] tablesToDelete = { "DEVICES", "device_messages" };


    int nextDeviceId = 100;

    public TestEndToEndWithAbstractedKafka() {
        super();

        try {
            p  = new PowerCoEmulator();
            d = new DeviceEmulator();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    @BeforeAll
    static void setUpBeforeClass() throws Exception {



    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {

    }

    @BeforeEach
    void setUp() throws Exception {


        c = connectVoltDB("localhost");

        for (String element : tablesToDelete) {
            c.callProcedure("@AdHoc", "DELETE FROM " + element + ";");
        }

    }

    @AfterEach
    void tearDown() throws Exception {


        c.drain();
        c.close();
        c = null;
    }


    @Test
    void testWithPowerco() {

        final long recordId = System.currentTimeMillis();

        try {


            // Create a generic meter
            long deviceId = testProvison(TestSendDownstreamWithVolt.TEST_OWNER,ReferenceData.TEST_JSON_METER_NAME);


            long externallMessageId = recordId;
            long latencyMs = -1;
            String errorMessage = null;
            Date createDate = null;
            int destinationSegmentId = -1;
            String featureName = "NIGHTSETTING";
            boolean enabled = true;

            //
            // Pretend to be powerco
            //

            EnableFeatureMessage originalMessage = new EnableFeatureMessage(deviceId, externallMessageId, latencyMs,
                    errorMessage, createDate, destinationSegmentId, featureName, enabled, 1);



            p.sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER, originalMessage);

            //
            // Pretend to be a meter
            //

            EnableFeatureMessage recoveredMessage = (EnableFeatureMessage) d.receiveDeviceMessage(ReferenceData.SEGMENT_1_TOPIC,
                    originalMessage.getExternallMessageId(),ReferenceData.getdeviceEncoding(originalMessage));

            compareOriginalAndAcceptedEnableFeatureMessages(originalMessage, recoveredMessage);

            d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, recoveredMessage);

            //
            // Pretend to be powerco
            //

            d.flush();
            p.flush();

            EnableFeatureMessage endStateMessage = (EnableFeatureMessage) p.receiveJsonPowercoMessage(ReferenceData.POWERCO_1_TOPIC,
                    originalMessage.getExternallMessageId());

            if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE + "")) {
                fail("Expected " + ReferenceData.MESSAGE_DONE + ", got " + endStateMessage.getErrorMessage());

            }

        } catch (Exception e) {
            msg(e.getMessage());
            fail(e);
        }

    }

    @Test
    void testWithPowercoAndTabDelim() {

        final long recordId = System.currentTimeMillis();

        try {


            // Create a generic meter
            long deviceId = testProvison(TestSendDownstreamWithVolt.TEST_OWNER,ReferenceData.TEST_DELIM_METER_NAME);
            long externallMessageId = recordId;
            long latencyMs = -1;
            String errorMessage = null;
            Date createDate = null;
            int destinationSegmentId = -1;
            String featureName = "NIGHTSETTING";
            boolean enabled = true;

            //
            // Pretend to be powerco
            //

            EnableFeatureMessage originalMessage = new EnableFeatureMessage(deviceId, externallMessageId, latencyMs,
                    errorMessage, createDate, destinationSegmentId, featureName, enabled, 1);

            p.sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER, originalMessage);

            //
            // Pretend to be a meter
            //

            EnableFeatureMessage recoveredMessage = (EnableFeatureMessage) d.receiveDeviceMessage(ReferenceData.SEGMENT_1_TOPIC,
                    originalMessage.getExternallMessageId(),ReferenceData.getdeviceEncoding(originalMessage));

            compareOriginalAndAcceptedEnableFeatureMessages(originalMessage, recoveredMessage);

            d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, recoveredMessage);

            //
            // Pretend to be powerco
            //

            d.flush();
            p.flush();

            EnableFeatureMessage endStateMessage = (EnableFeatureMessage) p.receiveJsonPowercoMessage(ReferenceData.POWERCO_1_TOPIC,
                    originalMessage.getExternallMessageId());

            if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE + "")) {
                fail("Expected " + ReferenceData.MESSAGE_DONE + ", got " + endStateMessage.getErrorMessage());

            }

        } catch (Exception e) {
            msg(e.getMessage());
            fail(e);
        }

    }
    private void compareOriginalAndAcceptedEnableFeatureMessages(EnableFeatureMessage originalMessage,
            EnableFeatureMessage recoveredMessage) {
        if (recoveredMessage.deviceId != originalMessage.deviceId) {
            fail("Device id mismatch");
        }

        if (recoveredMessage.getCreateDate() == null) {
            fail("Create Date is null");
        }

        if (recoveredMessage.latencyMs != -1) {
            fail("latencyMs set");
        }

        if (recoveredMessage.errorMessage != null) {
            fail("errorMessage set");
        }

        if (recoveredMessage.destinationSegmentId == -1) {
            fail("destinationSegmentId id mismatch");
        }

        if (!recoveredMessage.featureName.equals(originalMessage.featureName)) {
            fail("featureName mismatch");
        }

        if (!recoveredMessage.isEnabled()) {
            fail("isEnabled mismatch");
        }
    }

    private void checkResponseOK(ClientResponse cr) {
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            fail(cr.getStatusString());
        }

        if (cr.getAppStatus() != ReferenceData.OK) {
            fail(cr.getAppStatusString());
        }
    }





    long testProvison(long powerCo, String deviceName) {


        boolean done = false;

        while(! done) {
            nextDeviceId++;

            if (nextDeviceId % 2 == 0 && deviceName.equals(ReferenceData.TEST_DELIM_METER_NAME)) {
                done = true;
            } else if (nextDeviceId % 2 == 1 && deviceName.equals(ReferenceData.TEST_JSON_METER_NAME)) {
                done = true;
            }

        }




        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", nextDeviceId,
                    deviceName, TestSendDownstreamWithVolt.TEST_LOCATION, powerCo);

            checkResponseOK(cr);

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

        return nextDeviceId;
    }





    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
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

            msg("Connected to VoltDB");

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

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
