package org.voltse.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.voltse.edge.edgeencoders.JsonEncoderImpl;
import org.voltse.edge.edgeencoders.ModelEncoderIFace;
import org.voltse.edge.edgeencoders.TabEncoderImpl;
import org.voltse.edge.edgemessages.BaseMessage;
import org.voltse.edge.edgemessages.DisableFeatureMessage;
import org.voltse.edge.edgemessages.EnableFeatureMessage;
import org.voltse.edge.edgemessages.GetStatusMessage;
import org.voltse.edge.edgemessages.MessageIFace;
import org.voltse.edge.edgemessages.StartMessage;
import org.voltse.edge.edgemessages.StopMessage;
import org.voltse.edge.edgemessages.UpgradeFirmwareMessage;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import edgeprocs.ReferenceData;

class TestMessages {

    private static final String DELIM_CHAR = "\t";
    Gson g = new Gson();
    Random r = new Random();

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testGetStatusMessageJson() {
        try {
            // Make sure Json conversion works...

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(42);
            String mAsJson = m.asJson(g);

            GetStatusMessage m2 = (GetStatusMessage) BaseMessage.fromJson(mAsJson, g);

            String m2AsJson = m2.asJson(g);

            if (!m2AsJson.equals(mAsJson)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testGetStatusMessageDelim() {
        try {
            // Make sure Json conversion works...

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(42);
            m.setJsonPayload("FOO");

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            GetStatusMessage m2 = GetStatusMessage.fromDelimited(DELIM_CHAR, mAsDelim);

            String m2AsDelim = m2.asDelimited(DELIM_CHAR).toString();

            if (!mAsDelim.equals(m2AsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testGetStatusMessageDelimObject() {
        try {
            // Make sure Json conversion works...

            GetStatusMessage m = new GetStatusMessage();
            m.setDeviceId(42);
            m.setJsonPayload("FOO");

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            String className = BaseMessage.delimitedMessageType(DELIM_CHAR + "", mAsDelim);

            MessageIFace newObject = (MessageIFace) Class.forName(ReferenceData.EDGEMESSAGES + className).newInstance();

            newObject.setInternals(mAsDelim.split(DELIM_CHAR + ""));

            String mAsDelim2 = newObject.asDelimited(DELIM_CHAR).toString();

            if (!mAsDelim.equals(mAsDelim2)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testDisableFeatureMessageJson() {
        try {
            // Make sure Json conversion works...

            DisableFeatureMessage m = new DisableFeatureMessage();
            setInternals(m);
            String mAsJson = m.asJson(g);

            DisableFeatureMessage m2 = (DisableFeatureMessage) BaseMessage.fromJson(mAsJson, g);

            String m2AsJson = m2.asJson(g);

            if (!m2AsJson.equals(mAsJson)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testDisableFeatureMessageDelimited() {
        try {
            // Make sure Delim conversion works...

            DisableFeatureMessage m = new DisableFeatureMessage();

            m.enabled = false;

            setInternals(m);

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            DisableFeatureMessage m2 = DisableFeatureMessage.fromDelimited(DELIM_CHAR, mAsDelim);

            String m2AsDelim = m2.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testDisableFeatureMessageDelimitedObject() {
        try {
            // Make sure Delim conversion works...

            DisableFeatureMessage m = new DisableFeatureMessage();

            m.enabled = false;

            setInternals(m);

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            String className = BaseMessage.delimitedMessageType(DELIM_CHAR + "", mAsDelim);
            MessageIFace newObject = (MessageIFace) Class.forName(ReferenceData.EDGEMESSAGES + className).newInstance();
            newObject.setInternals(mAsDelim.split(DELIM_CHAR + ""));
            String m2AsDelim = newObject.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testEnableFeatureMessageDelimited() {
        try {
            // Make sure Delim conversion works...

            EnableFeatureMessage m = new EnableFeatureMessage();

            m.enabled = true;

            setInternals(m);

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            EnableFeatureMessage m2 = EnableFeatureMessage.fromDelimited(DELIM_CHAR, mAsDelim);

            String m2AsDelim = m2.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testEnableFeatureMessageDelimitedObject() {
        try {
            // Make sure Delim conversion works...

            EnableFeatureMessage m = new EnableFeatureMessage();

            m.enabled = true;

            setInternals(m);

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            String className = BaseMessage.delimitedMessageType(DELIM_CHAR + "", mAsDelim);
            MessageIFace newObject = (MessageIFace) Class.forName(ReferenceData.EDGEMESSAGES + className).newInstance();
            newObject.setInternals(mAsDelim.split(DELIM_CHAR + ""));
            String m2AsDelim = newObject.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testStartMessageJson() {
        try {
            // Make sure Json conversion works...

            StartMessage m = new StartMessage();
            setInternals(m);
            String mAsJson = m.asJson(g);

            StartMessage m2 = (StartMessage) BaseMessage.fromJson(mAsJson, g);

            String m2AsJson = m2.asJson(g);

            if (!m2AsJson.equals(mAsJson)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testStartMessageDelim() {
        try {
            // Make sure Json conversion works...

            StartMessage m = new StartMessage();
            setInternals(m);
            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            StartMessage m2 = StartMessage.fromDelimited(DELIM_CHAR, mAsDelim);

            String m2AsDelim = m2.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testStartMessageDelimObject() {
        try {
            // Make sure Json conversion works...

            StartMessage m = new StartMessage();
            setInternals(m);
            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            StartMessage m2 = StartMessage.fromDelimited(DELIM_CHAR, mAsDelim);

            String className = BaseMessage.delimitedMessageType(DELIM_CHAR + "", mAsDelim);
            MessageIFace newObject = (MessageIFace) Class.forName(ReferenceData.EDGEMESSAGES + className).newInstance();
            newObject.setInternals(mAsDelim.split(DELIM_CHAR + ""));
            String m2AsDelim = newObject.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testStopMessageDelim() {
        try {
            // Make sure Json conversion works...

            StopMessage m = new StopMessage();
            setInternals(m);
            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            StopMessage m2 = StopMessage.fromDelimited(DELIM_CHAR, mAsDelim);

            String m2AsDelim = m2.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testStopMessageDelimObject() {
        try {
            // Make sure Json conversion works...

            StopMessage m = new StopMessage();
            setInternals(m);
            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            String className = BaseMessage.delimitedMessageType(DELIM_CHAR + "", mAsDelim);
            MessageIFace newObject = (MessageIFace) Class.forName(ReferenceData.EDGEMESSAGES + className).newInstance();
            newObject.setInternals(mAsDelim.split(DELIM_CHAR + ""));
            String m2AsDelim = newObject.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testStopMessageJson() {
        try {
            // Make sure Json conversion works...

            StopMessage m = new StopMessage();
            setInternals(m);
            String mAsJson = m.asJson(g);

            StopMessage m2 = (StopMessage) BaseMessage.fromJson(mAsJson, g);

            String m2AsJson = m2.asJson(g);

            if (!m2AsJson.equals(mAsJson)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testUpgradeFirmwareMessageDelim() {
        try {
            // Make sure Json conversion works...

            byte[] TESTPAYLOAD = "Hello World".getBytes();

            UpgradeFirmwareMessage m = new UpgradeFirmwareMessage();
            setInternals(m);
            m.setPayload(TESTPAYLOAD);
            m.setCreateDate(new Date());
            m.setMessage("TEST_MESSAGE");
            m.setEnabled(false);

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            UpgradeFirmwareMessage m2 = UpgradeFirmwareMessage.fromDelimited(DELIM_CHAR, mAsDelim);

            String m2AsDelim = m2.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testUpgradeFirmwareMessageDelimObject() {
        try {
            // Make sure Json conversion works...

            byte[] TESTPAYLOAD = "Hello World".getBytes();

            UpgradeFirmwareMessage m = new UpgradeFirmwareMessage();
            setInternals(m);
            m.setPayload(TESTPAYLOAD);
            m.setCreateDate(new Date());
            m.setMessage("TEST_MESSAGE");
            m.setEnabled(false);

            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();

            String className = BaseMessage.delimitedMessageType(DELIM_CHAR + "", mAsDelim);
            MessageIFace newObject = (MessageIFace) Class.forName(ReferenceData.EDGEMESSAGES + className).newInstance();
            newObject.setInternals(mAsDelim.split(DELIM_CHAR + ""));
            String m2AsDelim = newObject.asDelimited(DELIM_CHAR).toString();

            if (!m2AsDelim.equals(mAsDelim)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

    @Test
    void testUpgradeFirmwareMessageJson() {
        try {
            // Make sure Json conversion works...

            byte[] TESTPAYLOAD = "Hello World".getBytes();

            UpgradeFirmwareMessage m = new UpgradeFirmwareMessage();
            setInternals(m);
            m.setPayload(TESTPAYLOAD);
            m.setCreateDate(new Date());
            m.setMessage("TEST_MESSAGE");
            m.setEnabled(false);

            String mAsJson = m.asJson(g);

            UpgradeFirmwareMessage m2 = (UpgradeFirmwareMessage) BaseMessage.fromJson(mAsJson, g);

            String m2AsJson = m2.asJson(g);

            if (!m2AsJson.equals(mAsJson)) {
                fail("Not equal");
            }

        } catch (Exception e) {
            fail(e);
        }

    }

//    @Test
//    void testUpgradeFirmwareMessageJson() {
//        try {
//            // Make sure Json conversion works...
//
//            UpgradeFirmwareMessage m = new UpgradeFirmwareMessage();
//
//
//            setInternals(m);
//            m.setPayload("FRED".getBytes());
//
//            String mAsDelim = m.asDelimited(DELIM_CHAR).toString();
//
//            UpgradeFirmwareMessage m2 = (UpgradeFirmwareMessage) UpgradeFirmwareMessage.fromDelimited(DELIM_CHAR, mAsDelim);
//
//            String m2AsDelim = m2.asDelimited(DELIM_CHAR).toString();
//
//
//
//            if (!mAsDelim.equals(m2AsDelim)) {
//                fail("Not equal");
//            }
//
//        } catch (Exception e) {
//            fail(e);
//        }
//
//    }
//
//    @Test
//    void testUpgradeFirmwareMessageDelim() {
//        try {
//            // Make sure Json conversion works...
//
//            UpgradeFirmwareMessage m = new UpgradeFirmwareMessage();
//            setInternals(m);
//            m.setPayload("FRED".getBytes());
//
//            String mAsJson = m.asJson(g);
//
//            UpgradeFirmwareMessage m2 = (UpgradeFirmwareMessage) UpgradeFirmwareMessage.fromJson(mAsJson, g);
//
//            String m2AsJson = m2.asJson(g);
//
//
//
//
//            String className = BaseMessage.delimitedMessageType(DELIM_CHAR+"", mAsDelim);
//            MessageIFace newObject =    (MessageIFace) Class.forName("edgemessages."+className).newInstance();
//            newObject.setInternals(mAsDelim.split(DELIM_CHAR+""));
//            String  m2AsDelim = newObject.asDelimited(DELIM_CHAR).toString();
//
//
//            if (!m2AsJson.equals(mAsJson)) {
//                fail("Not equal");
//            }
//
//        } catch (Exception e) {
//            fail(e);
//        }
//
//    }
//
//    @Test
//    void testUpgradeFirmwareMessageDelimObject() {
//
//        fail("not done");
//        try {
//            // Make sure Json conversion works...
//
//            UpgradeFirmwareMessage m = new UpgradeFirmwareMessage();
//            setInternals(m);
//            m.setPayload("FRED".getBytes());
//            String mAsJson = m.asJson(g);
//
//            UpgradeFirmwareMessage m2 = (UpgradeFirmwareMessage) UpgradeFirmwareMessage.fromJson(mAsJson, g);
//
//            String m2AsJson = m2.asJson(g);
//
//            if (!m2AsJson.equals(mAsJson)) {
//                fail("Not equal");
//            }
//
//        } catch (Exception e) {
//            fail(e);
//        }
//
//    }
//

    @Test
    void testCreateFromJson() {

        GetStatusMessage m = new GetStatusMessage();
        m.setDeviceId(42);
        MessageIFace ourMessage;

        try {
            ourMessage = (MessageIFace) BaseMessage.fromJson(m.asJson(g), g);

            msg(ourMessage.toString());

        } catch (JsonSyntaxException e) {
            fail(e);
        } catch (ClassNotFoundException e) {
            fail(e);
        }
    }

    @Test
    void testEncode() {
        try {

            ModelEncoderIFace[] encoders = { new JsonEncoderImpl(), new TabEncoderImpl() };

            for (ModelEncoderIFace encoder : encoders) {

                GetStatusMessage m = new GetStatusMessage();
                m.setDeviceId(42);
                m.setJsonPayload("fred");

                String mEndcoded = encoder.encode(m);

                GetStatusMessage m2 = (GetStatusMessage) encoder.decode(mEndcoded);

                if (!m2.asJson(g).equals(m2.asJson(g))) {
                    fail("Encode failure");
                }

            }
        } catch (Exception e) {
            fail(e);
        }
    }

    private void setInternals(BaseMessage m) {
        m.internalMessageId = r.nextLong();
        m.latencyMs = r.nextLong();
        m.errorMessage = "TEST" + r.nextLong();
        m.destinationSegmentId = r.nextInt(10) + 1;
        m.createDate = new Date(r.nextInt(1000));

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
