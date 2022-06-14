package com.polestar.ndcrefill;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Main {

//    private static final String HOST = "jdbc:postgresql://rds-manager.polestar-testing.local:5432/";
//    private static final String MASTER_USERNAME = "test_pan_dc";
//    private static final String MASTER_PASSWORD = "Vod2FrovyerUc8";
//-----------------------------------------------------------------------
//    private static final String HOST = "jdbc:postgresql://10.0.0.42:5432/";
//    private static final String MASTER_USERNAME = "lrit";
//    private static final String MASTER_PASSWORD = "ynf97xp";
//-----------------------------------------------------------------------
    private static final String HOST = "jdbc:postgresql://dev-encrypted-rds-manager.cp2beydjt4o5.us-east-1.rds.amazonaws.com:5432/";
    private static final String MASTER_USERNAME = "dev_pan_dc";
    private static final String MASTER_PASSWORD = "Vod2FrovyerUc8";

    private static final boolean DO_INSERTS = false;
    private static final boolean UPDATE_GENERATOR = false;
    private static final String FILE_LOCATION = "src/main/resources/missing_positions-test.csv";
    private static final String FLAG_CODE = "1108";
    private static final String DATA_CENTER_IDENTIFIER = "3108";

//-----------------------------------------------------------------------

    static Gson gson = new GsonBuilder().create();
    private static int shipPositionID;
    private static int messageID;

    public static void main(String[] args) {

        logStartingDetails();

        Connection conn;
        try {
            conn = DriverManager.getConnection(HOST, MASTER_USERNAME, MASTER_PASSWORD);
            Statement statement = conn.createStatement();

            getStartingDetails(statement);

            createOrUpdatePSQLFunction(statement);

            logLineBreak();

            //get position data for DC
            FileService fileService = new FileService(FLAG_CODE);
            List<Position> positions = fileService.processFile(FILE_LOCATION);
            int totalPositions = positions.size();
            AtomicInteger count = new AtomicInteger();
            Timestamp starttimestamp = new Timestamp(System.currentTimeMillis());

            logLineBreak();
            log.info("Start: " + starttimestamp);
            logLineBreak();

            //iterate through data
            positions.forEach(position -> {

                String insertStatement = constructSQLQuery(position);
                //execute
                try {
                    log.debug(gson.toJson(position));

                    if(DO_INSERTS){
                        statement.addBatch(insertStatement);
                    }
                    if(count.getAndIncrement() % 100 == 99){
                        int[] result = statement.executeBatch();
                        log.debug(gson.toJson(result));
                        log.info("Current position: " + count + " out of " + totalPositions);
                    }

                } catch (SQLException e) {
                    log.error("Error with SQL Batch", e);
                }
            });
            try{
                int[] results = statement.executeBatch();
                log.debug(gson.toJson(results));
                log.info("Current position: " + count + " out of " + totalPositions);
            } catch (SQLException e) {
                log.error("Error with SQL Batch", e);
            }


            try {
                updateGeneratorTable(statement);
            } catch (SQLException e) {
                log.error("Error with Updating Generator Tables", e);
            }


            conn.close();

            Timestamp endTimestamp = new Timestamp(System.currentTimeMillis());
            logLineBreak();
            log.info("finished: " + endTimestamp);
            log.info("time taken: " + Duration.between(starttimestamp.toInstant(), endTimestamp.toInstant()).toString());
            logLineBreak();

        } catch (SQLException e) {
            log.error("SQL Error", e);
        }
    }

    private static void createOrUpdatePSQLFunction(Statement statement) throws SQLException {
        logLineBreak();
        log.info("Updating Insert Positions function");
        String sql = "CREATE OR REPLACE FUNCTION insertposition(ship_position_id TEXT, imo_input TEXT, createTimestamp TEXT, speed_input TEXT, dc_id TEXT, dup_id TEXT, heading_input TEXT, latitude_input TEXT, longitude_input TEXT, gnssTimestamp TEXT, message_id TEXT) RETURNS TEXT AS $$\n" +
                "      BEGIN\n" +
                "\tIF NOT EXISTS (SELECT id FROM shipposition WHERE shipborneequipmenttimestamp = gnssTimestamp and shipborneequipmentidentifier = (select shipborneequipmentidentifier from ship where imonumber = imo)) THEN\n" +
                "  START TRANSACTION;\n" +
                "    INSERT INTO shipposition(id, aspidentifier, aspreceivetimestamp, asptransmittimestamp, averagespeed, datacenteridentifier, datauserprovideridentifier, heading, mmsi, latitude, longitude, receivetimestamp, shipname, shippositiontype, shipborneequipmentidentifier, shipborneequipmenttimestamp, speed, version, ship_id) select\n" +
                "    ship_position_id, '4001', createTimestamp, createTimestamp, speed_input, dc_id, dup_id, heading_input, (select mmsi from ship where imonumber = imo_input), latitude_input, longitude_input, createTimestamp, (select shipname from ship where imonumber = imo_input), 'PERIODIC_REPORT', (select shipborneequipmentidentifier from ship where imonumber = imo_input), gnssTimestamp, speed_input, '0', (select id from ship where imonumber = imo_input) ;\n" +
                "\n" +
                "    INSERT INTO message(id, messagestate, messagetype, transmittimestamp, version, test)\n" +
                "    VALUES (message_id, 'NOT_SEND', 'PERIODIC_REPORT', createTimestamp,1, false);\n" +
                "\n" +
                "    INSERT INTO positionreportmessage(id, shipposition_id, datauserrequestoridentifier, responsetype)\n" +
                "    VALUES (message_id, ship_position_id, dup_id, 'FLAG');\n" +
                "\n" +
                "  COMMIT;\n" +
                "\n" +
                "\tEND IF;\n" +
                "\n" +
                "      RETURN ship_position_id;\n" +
                "   END; $$\n" +
                "   LANGUAGE plpgsql;";
        statement.execute(sql);
        log.info("Updated Successfully");
    }

    private static void updateGeneratorTable(Statement statement) throws SQLException {
        logLineBreak();

        int newMaxShipPositionID = getMaxShipPositionID(statement);
        log.info("new max shippositionID: " + newMaxShipPositionID);
        int newShipPositionGeneratorValue = (int) Math.ceil(newMaxShipPositionID/50) + 100;
        log.info("new ship position generator value " + newShipPositionGeneratorValue);

        if(UPDATE_GENERATOR) {
            statement.execute("Update Generator set sequence_next_hi_value=" + newShipPositionGeneratorValue + " where sequence_name='ShipPosition';");
            log.info("Updated Ship Position Generator value successfully");
        } else{
            log.info("Did not update ship position generator value, UPDATE_GENERATOR is false");
        }

        logLineBreak();

        int newMaxMessageID = getMaxMessageID(statement);
        log.info("new max messageID: " + newMaxMessageID);
        int newMessageGeneratorValue = (int) Math.ceil(newMaxMessageID/50) + 100;
        log.info("new message generator value " + newMessageGeneratorValue);
        if(UPDATE_GENERATOR) {
            statement.execute("Update Generator set sequence_next_hi_value=" + newMessageGeneratorValue + " where sequence_name='Message';");
            log.info("Updated Message Generator value successfully");
        } else {
            log.info("Did not update message generator value, UPDATE_GENERATOR is false");
        }
    }

    private static void logStartingDetails() {
        logLineBreak();
        log.info("Starting Backfill");

        log.info("HOST: " + HOST);

        log.info("MASTER_USERNAME: " + MASTER_USERNAME);

        log.info("DO_INSERTS: " + DO_INSERTS);

        log.info("FILE_LOCATION: " + FILE_LOCATION);

        log.info("FLAG_CODE: " + FLAG_CODE);

        log.info("DATA_CENTER_IDENTIFIER: " + DATA_CENTER_IDENTIFIER);
    }

    private static void getStartingDetails(Statement statement) throws SQLException {
        logLineBreak();
        log.info("Starting Details");
        log.info("ShipPosition Generator Value: " + getShipPositionGeneratorValue(statement));
        log.info("Message Generator Value: " + getMessageGeneratorValue(statement));
        int maxShipPositionID = getMaxShipPositionID(statement);
        shipPositionID = maxShipPositionID + 2000;
        log.info("current max ship position id is: " + maxShipPositionID + " starting shipPositionID: " + shipPositionID);

        int maxMessageID = getMaxMessageID(statement);
        messageID = maxMessageID + 2000;
        log.info("current max message id is: " + maxMessageID + " starting messageID: " + messageID);
    }

    private static int getShipPositionGeneratorValue(Statement statement) throws SQLException {
        ResultSet out = statement.executeQuery("SELECT sequence_next_hi_value AS shipPositionTotal from generator where sequence_name='ShipPosition'");
        out.next();
        return out.getInt("shipPositionTotal");
    }

    private static int getMessageGeneratorValue(Statement statement) throws SQLException {
        ResultSet out = statement.executeQuery("SELECT sequence_next_hi_value AS shipPositionTotal from generator where sequence_name='Message'");
        out.next();
        return out.getInt("shipPositionTotal");
    }

    private static int getMaxShipPositionID(Statement statement) throws SQLException {
        //get starting count
        ResultSet out = statement.executeQuery("SELECT max(id) AS maxID from shipposition");
        out.next();
        return out.getInt("maxID");
    }

    private static int getMaxMessageID(Statement statement) throws SQLException {
        //get starting count
        ResultSet out = statement.executeQuery("SELECT max(id) AS maxID from message");
        out.next();
        return out.getInt("maxID");
    }

    private static String constructSQLQuery(Position position) {
        String createTimestamp = String.format("%s %s", position.getCreation_date(), position.getCreation_time());
        String gnssTimestamp = String.format("%s %s", position.getTrail_date(), position.getTrail_time());

        String sql = String.format(
                "insert into shipposition " +
                "(id, aspidentifier, aspreceivetimestamp, asptransmittimestamp, averagespeed, datacenteridentifier, datauserprovideridentifier, heading, mmsi, latitude, longitude, receivetimestamp, shipname, shippositiontype, shipborneequipmentidentifier, shipborneequipmenttimestamp, speed, version, ship_id) " +
                "select " +
                "'%d', '4001', '%s', '%s', '%s', '%s', '%s', '%s', (select mmsi from ship where imonumber = '%s'), '%s', '%s', '%s', (select shipname from ship where imonumber = '%s'), 'PERIODIC_REPORT', (select shipborneequipmentidentifier from ship where imonumber = '%s'), '%s', '%s', '0', (select id from ship where imonumber = '%s') " +
                "WHERE NOT EXISTS (SELECT id FROM shipposition WHERE shipborneequipmenttimestamp = '%s' and shipborneequipmentidentifier = (select shipborneequipmentidentifier from ship where imonumber = '%s'));",
                shipPositionID, createTimestamp, createTimestamp, position.getTrail_speed(), DATA_CENTER_IDENTIFIER, position.getDC_ID(), position.getTrail_heading(), position.getI_m_o_number(), position.getTrail_latitude(), position.getTrail_longitude(), createTimestamp, position.getI_m_o_number(), position.getI_m_o_number(), gnssTimestamp, position.getTrail_speed(), position.getI_m_o_number(), gnssTimestamp, position.getI_m_o_number());
        shipPositionID++;
        return sql;
    }


    private static String query(Position position){

        String createTimestamp = String.format("%s %s", position.getCreation_date(), position.getCreation_time());
        String gnssTimestamp = String.format("%s %s", position.getTrail_date(), position.getTrail_time());

//        insertposition(ship_position_id TEXT, imo_input TEXT, createTimestamp TEXT, speed_input TEXT, dc_id TEXT, dup_id TEXT, heading_input TEXT, latitude_input TEXT, longitude_input TEXT, gnssTimestamp TEXT, message_id TEXT)

//        INSERT INTO shipposition(id, aspidentifier, aspreceivetimestamp, asptransmittimestamp, averagespeed, datacenteridentifier, datauserprovideridentifier, heading, mmsi, latitude, longitude, receivetimestamp, shipname, shippositiontype, shipborneequipmentidentifier, shipborneequipmenttimestamp, speed, version, ship_id)
//        select
//        ship_position_id, '4001', createTimestamp, createTimestamp, speed_input, dc_id, dup_id, heading_input, (select mmsi from ship where imonumber = imo_input), latitude_input, longitude_input, createTimestamp, (select shipname from ship where imonumber = imo_input), 'PERIODIC_REPORT', (select shipborneequipmentidentifier from ship where imonumber = imo_input), gnssTimestamp, speed_input, '0', (select id from ship where imonumber = imo_input) ;

        String sql = String.format(
                "SELECT insertposition('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d')",
                shipPositionID, position.getI_m_o_number(), createTimestamp, position.getTrail_speed(), DATA_CENTER_IDENTIFIER, position.getDC_ID(), position.getTrail_heading(), position.getTrail_latitude(), position.getTrail_longitude(), gnssTimestamp, messageID);
        shipPositionID++;
        messageID++;
        return sql;

    }

    private static void logLineBreak(){
        log.info("-----------------------------------------------------------------");
    }

}
