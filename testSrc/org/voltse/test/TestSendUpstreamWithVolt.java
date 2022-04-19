package org.voltse.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Base64;
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
import org.voltse.edge.edgemessages.GetStatusMessage;

import com.google.gson.Gson;

import edgeprocs.ReferenceData;

class TestSendUpstreamWithVolt {
    Client c;
    Gson g = new Gson();

    String[] tablesToDelete = { "DEVICES", "device_messages" };

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
    }

    @Test
    void testUpstream() {

        try {

            GetStatusMessage m = creatTestMessage();

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.OK) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    @Test
    void testUpstreamBuf() {

        try {

            GetStatusMessage m = creatTestMessage();

            c.callProcedure("@AdHoc",
                    "UPDATE device_messages SET status_code = 'BUF' WHERE device_id = " + m.getDeviceId() + ";");

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_MESSAGE_NOT_IN_FLIGHT_IS_ACTIVE) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    @Test
    void testUpstreamIdDeviceMismatch() {

        try {

            GetStatusMessage m = creatTestMessage();
            m.setDeviceId(TestSendDownstreamWithVolt.GENERIC_DEVICE_ID + 1);

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_DEVICE_ID_MISMATCH) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    @Test
    void testUpstreamIdBadDevice() {

        try {

            GetStatusMessage m = creatTestMessage();

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID + 1,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_UNKNOWN_DEVICE) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    @Test
    void testUpstreamExternalId() {

        try {

            GetStatusMessage m = creatTestMessage();
            m.setExternallMessageId(0);

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_MISSING_EXTERNAL_MESSAGE_ID) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    @Test
    void testUpstreamInternalId() {

        try {

            GetStatusMessage m = creatTestMessage();
            m.setInternalMessageId(0);

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_MISSING_INTERNAL_MESSAGE_ID) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    @Test
    void testUpstreamNoInternalId() {

        try {

            GetStatusMessage m = creatTestMessage();
            m.setInternalMessageId(0);

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_MISSING_INTERNAL_MESSAGE_ID) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    @Test
    void testUpstreamNoExternalId() {

        try {

            GetStatusMessage m = creatTestMessage();
            m.setExternallMessageId(0);

            ClientResponse cr = c.callProcedure("SendMessageUpstream", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    m.getDeviceId() + "," + Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_MISSING_EXTERNAL_MESSAGE_ID) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    void testProvison() {

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", TestSendDownstreamWithVolt.GENERIC_DEVICE_ID,
                    ReferenceData.TEST_JSON_METER_NAME, TestSendDownstreamWithVolt.TEST_LOCATION,
                    TestSendDownstreamWithVolt.TEST_OWNER);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.OK) {
                fail(cr.getAppStatusString());
            }

        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }

    }

    public GetStatusMessage creatTestMessage() {

        testProvison();

        GetStatusMessage m = null;

        try {

            m = new GetStatusMessage();
            m.setDeviceId(TestSendDownstreamWithVolt.GENERIC_DEVICE_ID);
            m.setExternallMessageId(TestSendDownstreamWithVolt.GENERIC_EXTERNAL_MESSAGE_ID);

            ClientResponse cr = c.callProcedure("SendMessageDownstream", m.getDeviceId(),
                    TestSendDownstreamWithVolt.TEST_OWNER, Base64.getEncoder().encodeToString(m.asJson(g).getBytes()));

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.OK) {
                fail(cr.getAppStatusString());
            }

            ClientResponse cr2 = c.callProcedure("GetDevice", m.getDeviceId(), m.getDeviceId());

            if (cr2.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            cr2.getResults()[1].advanceRow();
            m.setInternalMessageId(cr2.getResults()[1].getLong("internal_message_id"));

        } catch (Exception e) {
            fail(e.getMessage());
        }

        return m;

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
