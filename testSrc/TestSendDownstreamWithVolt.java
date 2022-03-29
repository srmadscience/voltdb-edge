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
import org.voltse.edge.edgemessages.GetStatusMessage;

import com.google.gson.Gson;

import edgeprocs.ReferenceData;

class TestSendDownstreamWithVolt {

    public static final int GENERIC_DEVICE_ID = 42;
    public static final long GENERIC_BAD_DEVICE_ID = 41;

    public static final int GENERIC_EXTERNAL_MESSAGE_ID = 415;


    public final static String TEST_METER_NAME = "MeterTron100";
    public final String TEST_DELIM_METER_NAME = "HomeMeter100";
    public final static long TEST_LOCATION = 1;
    public final static long TEST_OWNER = 1;

    public final String TEST_BAD_METER_NAME = "NOT MeterTron100";
    public final long TEST_BAD_LOCATION = -1;
    public final long TEST_BAD_OWNER = -1;

       Client c;
    Gson g = new Gson();

    String[] tablesToDelete = { "DEVICES","device_messages" };

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
            c.callProcedure("@AdHoc", "DELETE FROM " + element+";");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        c.drain();
        c.close();
    }

    @Test
    void testDownstream() {

        testProvison() ;

        try {

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(GENERIC_DEVICE_ID);
            m.setExternallMessageId(GENERIC_EXTERNAL_MESSAGE_ID);

            ClientResponse cr = c.callProcedure("SendMessageDownstream", m.getDeviceId(),  TEST_OWNER, m.getMessageType(), m.asJson(g) );

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.OK) {
                fail(cr.getAppStatusString());
            }


        } catch (Exception e) {
            fail(e.getMessage());
        }


    }

    @Test
    void testDownstreamDeviceIdMismatch() {

        testProvison() ;

        try {

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(GENERIC_DEVICE_ID);
            m.setExternallMessageId(GENERIC_EXTERNAL_MESSAGE_ID);

            ClientResponse cr = c.callProcedure("SendMessageDownstream", m.getDeviceId() + 1,  TEST_OWNER, m.getMessageType(), m.asJson(g) );

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_DEVICE_ID_MISMATCH) {
                fail(cr.getAppStatusString());
            }


        } catch (Exception e) {
            fail(e.getMessage());
        }


    }


    @Test
    void testDownstreamDelim() {

        testProvisonOtherMeterKind() ;

        try {

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(GENERIC_DEVICE_ID);
            m.setExternallMessageId(GENERIC_EXTERNAL_MESSAGE_ID);

            ClientResponse cr = c.callProcedure("SendMessageDownstream", m.getDeviceId(),  TEST_OWNER, m.getMessageType(), m.asJson(g) );

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.OK) {
                fail(cr.getAppStatusString());
            }


        } catch (Exception e) {
            fail(e.getMessage());
        }


    }



    @Test
    void testDownstreamBadDevice() {

        testProvison() ;

        try {

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(GENERIC_BAD_DEVICE_ID);
            m.setExternallMessageId(GENERIC_EXTERNAL_MESSAGE_ID);

            ClientResponse cr = c.callProcedure("SendMessageDownstream", m.getDeviceId(),  TEST_OWNER, m.getMessageType(), m.asJson(g) );

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_UNKNOWN_DEVICE) {
                fail(cr.getAppStatusString());
            }


        } catch (Exception e) {
            fail(e.getMessage());
        }


    }



    @Test
    void testDownstreamTwice() {

        testProvison() ;


        try {

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(GENERIC_DEVICE_ID);
            m.setExternallMessageId(GENERIC_EXTERNAL_MESSAGE_ID);



            ClientResponse cr = c.callProcedure("SendMessageDownstream", m.getDeviceId(),  TEST_OWNER, m.getMessageType(), m.asJson(g) );

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.OK) {
                fail(cr.getAppStatusString());
            }

cr = c.callProcedure("SendMessageDownstream", m.getDeviceId(),  TEST_OWNER, m.getMessageType(), m.asJson(g) );

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_DUPLICATE_MESSAGE) {
                fail(cr.getAppStatusString());
            }


        } catch (Exception e) {
            fail(e.getMessage());
        }


    }


    @Test
    void testDownstreamBadOwner() {

        testProvison() ;

        try {

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(GENERIC_DEVICE_ID);
            m.setExternallMessageId(GENERIC_EXTERNAL_MESSAGE_ID);

            ClientResponse cr = c.callProcedure("SendMessageDownstream", m.getDeviceId(),  TEST_BAD_OWNER, m.getMessageType(), m.asJson(g) );

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_UNKNOWN_UTIL_CO) {
                fail(cr.getAppStatusString());
            }


        } catch (Exception e) {
            fail(e.getMessage());
        }


    }




    @Test
    void testProvison() {

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", GENERIC_DEVICE_ID, TEST_METER_NAME, TEST_LOCATION, TEST_OWNER);

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
    void testProvisonOtherMeterKind() {

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", GENERIC_DEVICE_ID, TEST_DELIM_METER_NAME, TEST_LOCATION, TEST_OWNER);

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
    void testProvisonTwice() {

        testProvison();

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", GENERIC_DEVICE_ID, TEST_METER_NAME, TEST_LOCATION, TEST_OWNER);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.DEVICE_ALREADY_EXISTS) {
                fail(cr.getAppStatusString());
            }


        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }


    }


    @Test
    void testProvisonBadLocation() {

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", GENERIC_DEVICE_ID, TEST_METER_NAME, TEST_BAD_LOCATION, TEST_OWNER);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_UNKNOWN_LOCATION) {
                fail(cr.getAppStatusString());
            }


        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }


    }

    @Test
    void testProvisonBadOwner() {

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", GENERIC_DEVICE_ID, TEST_METER_NAME, TEST_LOCATION, TEST_BAD_OWNER);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_UNKNOWN_UTIL_CO) {
                fail(cr.getAppStatusString());
            }


        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }


    }



    @Test
    void testProvisonBadMeterName() {

        try {
            ClientResponse cr = c.callProcedure("ProvisionDevice", GENERIC_DEVICE_ID, TEST_BAD_METER_NAME, TEST_LOCATION, TEST_OWNER);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                fail(cr.getStatusString());
            }

            if (cr.getAppStatus() != ReferenceData.ERROR_UNKNOWN_MODEL) {
                fail(cr.getAppStatusString());
            }


        } catch (IOException | ProcCallException e) {
            fail(e.getMessage());
        }


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
