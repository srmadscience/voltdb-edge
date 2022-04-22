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
import org.voltse.edge.edgemessages.DisableFeatureMessage;
import org.voltse.edge.edgemessages.EnableFeatureMessage;
import org.voltse.edge.edgemessages.GetStatusMessage;
import org.voltse.edge.edgemessages.MessageIFace;
import org.voltse.edge.edgemessages.StartMessage;
import org.voltse.edge.edgemessages.StopMessage;
import org.voltse.edge.edgemessages.UpgradeFirmwareMessage;

import edgeprocs.ReferenceData;

class TestEndToEndWithAbstractedKafka {

    final long startMs = System.currentTimeMillis();

    PowerCoEmulator p;
    DeviceEmulator d;
    Client c;

    String[] tablesToDelete = { "DEVICES", "device_messages" };

    int nextDeviceId = 100;

    public TestEndToEndWithAbstractedKafka() {
        super();

        try {
            p = new PowerCoEmulator();
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

        p.drain();
        d.drain();

    }

    @AfterEach
    void tearDown() throws Exception {

        c.drain();
        c.close();
        c = null;

    }

    @Test
    void smokeTestMany() {
        for (int i = 0; i < 10; i++) {
            testEnableFeature();
        }
    }

    @Test
    void testEnableFeature() {

        for (String element : ReferenceData.METER_TYPES) {

            msg(element);

            final long recordId = System.currentTimeMillis();

            try {

                // Create a generic meter
                long deviceId = provision(TestSendDownstreamWithVolt.TEST_OWNER, element);

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

                p.sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER,
                        originalMessage);

                //
                // Pretend to be a meter
                //

                EnableFeatureMessage recoveredMessage = (EnableFeatureMessage) d.receiveDeviceMessage(
                        originalMessage.getExternallMessageId(), ReferenceData.getdeviceEncoding(originalMessage));

                compareOriginalAndAcceptedEnableFeatureMessages(originalMessage, recoveredMessage);

                d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, recoveredMessage);

                //
                // Pretend to be powerco
                //

                EnableFeatureMessage endStateMessage = (EnableFeatureMessage) p
                        .receiveJsonPowercoMessage(originalMessage.getExternallMessageId());

                if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE_STRING + "")) {
                    fail("Expected " + ReferenceData.MESSAGE_DONE_STRING + ", got "
                            + endStateMessage.getErrorMessage());

                }

            } catch (Exception e) {
                msg(e.getMessage());
                fail(e);
            }

        }
    }

    @Test
    void testUpgradeFirmware() {

        for (String element : ReferenceData.METER_TYPES) {

            msg(element);

            final long recordId = System.currentTimeMillis();

            try {

                // Create a generic meter
                long deviceId = provision(TestSendDownstreamWithVolt.TEST_OWNER, element);

                long externallMessageId = recordId;
                long latencyMs = -1;
                String errorMessage = null;
                Date createDate = null;
                int destinationSegmentId = -1;

                byte[] payload = new byte[0];

                //
                // Pretend to be powerco
                //

                UpgradeFirmwareMessage originalMessage = new UpgradeFirmwareMessage(deviceId, externallMessageId,
                        latencyMs, errorMessage, createDate, destinationSegmentId, payload, 1);

                p.sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER,
                        originalMessage);

                //
                // Pretend to be a meter
                //

                UpgradeFirmwareMessage recoveredMessage = (UpgradeFirmwareMessage) d.receiveDeviceMessage(
                        originalMessage.getExternallMessageId(), ReferenceData.getdeviceEncoding(originalMessage));

                compareOriginalAndAcceptedFirmwareMessages(originalMessage, recoveredMessage);

                d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, recoveredMessage);

                //
                // Pretend to be powerco
                //

                UpgradeFirmwareMessage endStateMessage = (UpgradeFirmwareMessage) p
                        .receiveJsonPowercoMessage(originalMessage.getExternallMessageId());

                if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE_STRING + "")) {
                    fail("Expected " + ReferenceData.MESSAGE_DONE_STRING + ", got "
                            + endStateMessage.getErrorMessage());

                }

            } catch (Exception e) {
                msg(e.getMessage());
                fail(e);
            }

        }
    }

    @Test
    void testStartMessage() {

        for (String element : ReferenceData.METER_TYPES) {

            msg(element);

            final long recordId = System.currentTimeMillis();

            try {

                // Create a generic meter
                long deviceId = provision(TestSendDownstreamWithVolt.TEST_OWNER, element);

                long externallMessageId = recordId;
                long latencyMs = -1;
                String errorMessage = null;
                Date createDate = null;
                int destinationSegmentId = -1;

                boolean started = true;

                //
                // Pretend to be powerco
                //

                StartMessage originalMessage = new StartMessage(deviceId, externallMessageId, latencyMs, errorMessage,
                        createDate, destinationSegmentId, started, 1);

                d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, originalMessage);

                //
                // Pretend to be powerco
                //

                StartMessage endStateMessage = (StartMessage) p
                        .receiveJsonPowercoMessage(originalMessage.getExternallMessageId());

                if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE_STRING + "")) {
                    fail("Expected " + ReferenceData.MESSAGE_DONE_STRING + ", got "
                            + endStateMessage.getErrorMessage());

                }

            } catch (Exception e) {
                msg(e.getMessage());
                fail(e);
            }
        }

    }

    @Test
    void testStopMessage() {

        for (String element : ReferenceData.METER_TYPES) {

            msg(element);

            final long recordId = System.currentTimeMillis();

            try {

                // Create a generic meter
                long deviceId = provision(TestSendDownstreamWithVolt.TEST_OWNER, element);

                long externallMessageId = recordId;
                long latencyMs = -1;
                String errorMessage = null;
                Date createDate = null;
                int destinationSegmentId = -1;

                boolean started = true;

                //
                // Pretend to be powerco
                //

                StopMessage originalMessage = new StopMessage(deviceId, externallMessageId, latencyMs, errorMessage,
                        createDate, destinationSegmentId, started, 1);

                d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, originalMessage);

                //
                // Pretend to be powerco
                //

                StopMessage endStateMessage = (StopMessage) p
                        .receiveJsonPowercoMessage(originalMessage.getExternallMessageId());

                if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE_STRING + "")) {
                    fail("Expected " + ReferenceData.MESSAGE_DONE_STRING + ", got "
                            + endStateMessage.getErrorMessage());

                }

            } catch (Exception e) {
                msg(e.getMessage());
                fail(e);
            }
        }

    }

    @Test
    void testDisableFeature() {

        for (String element : ReferenceData.METER_TYPES) {

            msg(element);

            final long recordId = System.currentTimeMillis();

            try {

                // Create a generic meter
                long deviceId = provision(TestSendDownstreamWithVolt.TEST_OWNER, element);

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

                DisableFeatureMessage originalMessage = new DisableFeatureMessage(deviceId, externallMessageId,
                        latencyMs, errorMessage, createDate, destinationSegmentId, featureName, enabled, 1);

                p.sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER,
                        originalMessage);

                //
                // Pretend to be a meter
                //

                DisableFeatureMessage recoveredMessage = (DisableFeatureMessage) d.receiveDeviceMessage(
                        originalMessage.getExternallMessageId(), ReferenceData.getdeviceEncoding(originalMessage));

                compareOriginalAndAcceptedDisableFeatureMessages(originalMessage, recoveredMessage);

                d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, recoveredMessage);

                //
                // Pretend to be powerco
                //

                DisableFeatureMessage endStateMessage = (DisableFeatureMessage) p
                        .receiveJsonPowercoMessage(originalMessage.getExternallMessageId());

                if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE_STRING + "")) {
                    fail("Expected " + ReferenceData.MESSAGE_DONE_STRING + ", got "
                            + endStateMessage.getErrorMessage());

                }

            } catch (Exception e) {
                msg(e.getMessage());
                fail(e);
            }
        }

    }

    @Test
    void testGetStatus() {

        for (String element : ReferenceData.METER_TYPES) {

            msg(element);

            final long recordId = System.currentTimeMillis();

            try {

                // Create a generic meter
                long deviceId = provision(TestSendDownstreamWithVolt.TEST_OWNER, element);

                long externallMessageId = recordId;
                long latencyMs = -1;
                String errorMessage = null;
                Date createDate = null;
                int destinationSegmentId = -1;

                //
                // Pretend to be powerco
                //

                GetStatusMessage originalMessage = new GetStatusMessage(deviceId, externallMessageId, latencyMs,
                        errorMessage, createDate, destinationSegmentId, 1, null);

                p.sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER,
                        originalMessage);

                //
                // Pretend to be a meter
                //

                GetStatusMessage recoveredMessage = (GetStatusMessage) d.receiveDeviceMessage(
                        originalMessage.getExternallMessageId(), ReferenceData.getdeviceEncoding(originalMessage));

                compareOriginalAndAcceptedGetStatusMessageMessages(originalMessage, recoveredMessage);

                d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, recoveredMessage);

                //
                // Pretend to be powerco
                //

                GetStatusMessage endStateMessage = (GetStatusMessage) p
                        .receiveJsonPowercoMessage(originalMessage.getExternallMessageId());

                if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE_STRING + "")) {
                    fail("Expected " + ReferenceData.MESSAGE_DONE_STRING + ", got "
                            + endStateMessage.getErrorMessage());

                }

            } catch (Exception e) {
                msg(e.getMessage());
                fail(e);
            }
        }

    }

    private void compareOriginalAndAcceptedGetStatusMessageMessages(GetStatusMessage originalMessage,
            GetStatusMessage recoveredMessage) {

        compareOriginalAndAcceptedMessagesMessageIFace(originalMessage, recoveredMessage);

    }

    @Test
    void testWithTabDelim() {

        final long recordId = System.currentTimeMillis();

        try {

            // Create a generic meter
            long deviceId = provision(TestSendDownstreamWithVolt.TEST_OWNER, ReferenceData.TEST_DELIM_METER_NAME);
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

            msg("sendMessageDownstream");
            p.sendMessageDownstream(ReferenceData.DOWNSTREAM_TOPIC, TestSendDownstreamWithVolt.TEST_OWNER,
                    originalMessage);

            //
            // Pretend to be a meter
            //

            msg("receiveDeviceMessage");
            EnableFeatureMessage recoveredMessage = (EnableFeatureMessage) d.receiveDeviceMessage(
                    originalMessage.getExternallMessageId(), ReferenceData.getdeviceEncoding(originalMessage));

            compareOriginalAndAcceptedEnableFeatureMessages(originalMessage, recoveredMessage);

            msg("sendMessageUpstream");
            d.sendMessageUpstream(ReferenceData.UPSTREAM_TOPIC, recoveredMessage);

            //
            // Pretend to be powerco
            //

            msg("get endStateMessage");
            EnableFeatureMessage endStateMessage = (EnableFeatureMessage) p
                    .receiveJsonPowercoMessage(originalMessage.getExternallMessageId());

            if (!endStateMessage.getErrorMessage().equals(ReferenceData.MESSAGE_DONE_STRING + "")) {
                fail("Expected " + ReferenceData.MESSAGE_DONE_STRING + ", got " + endStateMessage.getErrorMessage());

            }

        } catch (Exception e) {
            msg(e.getMessage());
            fail(e);
        }

    }

    private void compareOriginalAndAcceptedMessagesMessageIFace(MessageIFace originalMessage,
            MessageIFace recoveredMessage) {

        if (recoveredMessage.getDeviceId() != originalMessage.getDeviceId()) {
            fail("Device id mismatch");
        }

        if (recoveredMessage.getCreateDate() == null) {
            fail("Create Date is null");
        }

        if (recoveredMessage.getLatencyMs() != -1) {
            fail("latencyMs set");
        }

        if (recoveredMessage.getErrorMessage() != null) {
            fail("errorMessage set");
        }

        if (recoveredMessage.getDestinationSegmentId() == -1) {
            fail("destinationSegmentId id mismatch");
        }

    }

    private void compareOriginalAndAcceptedEnableFeatureMessages(EnableFeatureMessage originalMessage,
            EnableFeatureMessage recoveredMessage) {

        compareOriginalAndAcceptedMessagesMessageIFace(originalMessage, recoveredMessage);

        if (!recoveredMessage.featureName.equals(originalMessage.featureName)) {
            fail("featureName mismatch");
        }

        if (!recoveredMessage.isEnabled()) {
            fail("isEnabled mismatch");
        }
    }

    private void compareOriginalAndAcceptedDisableFeatureMessages(DisableFeatureMessage originalMessage,
            DisableFeatureMessage recoveredMessage) {

        compareOriginalAndAcceptedMessagesMessageIFace(originalMessage, recoveredMessage);

        if (!recoveredMessage.featureName.equals(originalMessage.featureName)) {
            fail("featureName mismatch");
        }

        if (!recoveredMessage.isEnabled()) {
            fail("isEnabled mismatch");
        }
    }

    private void compareOriginalAndAcceptedFirmwareMessages(UpgradeFirmwareMessage originalMessage,
            UpgradeFirmwareMessage recoveredMessage) {

        compareOriginalAndAcceptedMessagesMessageIFace(originalMessage, recoveredMessage);

    }

    private void checkResponseOK(ClientResponse cr) {
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            fail(cr.getStatusString());
        }

        if (cr.getAppStatus() != ReferenceData.OK) {
            fail(cr.getAppStatusString());
        }
    }

    long provision(long powerCo, String deviceName) {

        boolean done = false;

        while (!done) {
            nextDeviceId++;

            if (nextDeviceId % 2 == 0 && deviceName.equals(ReferenceData.TEST_DELIM_METER_NAME)) {
                done = true;
            } else if (nextDeviceId % 2 == 1 && deviceName.equals(ReferenceData.TEST_JSON_METER_NAME)) {
                done = true;
            }

        }

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", nextDeviceId, deviceName,
                    TestSendDownstreamWithVolt.TEST_LOCATION, powerCo);

            checkResponseOK(cr);

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

        return nextDeviceId;
    }

    long provisionMany(int howMany, long powerCo) {

        msg("Creating " + howMany + " devices...");
        for (int i = 0; i < howMany; i++) {
            nextDeviceId++;

            try {
                ClientResponse cr = c.callProcedure("ProvisionDevice", nextDeviceId,
                        ReferenceData.METER_TYPES[(nextDeviceId + 1) % 2], TestSendDownstreamWithVolt.TEST_LOCATION,
                        powerCo);

                checkResponseOK(cr);

            } catch (IOException | ProcCallException e) {
                fail(e.getMessage());
            }

        }
        msg("Creating " + howMany + " devices...done");

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
