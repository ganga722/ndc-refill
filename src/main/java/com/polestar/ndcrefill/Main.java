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

    private static final String HOST = "jdbc:postgresql://rds-manager.polestar-testing.local:5432/";
    private static final String MASTER_USERNAME = "test_pan_dc";
    private static final String MASTER_PASSWORD = "Vod2FrovyerUc8";

    private static final boolean DO_INSERTS = false;

    private static final String FILE_LOCATION = "src/main/resources/missing_positions-test.csv";

    private static final String FLAG_CODE = "1108";

    private static final String DATA_CENTER_IDENTIFIER = "3108";
    private static int shipPositionID = 3000055;

    static Gson gson = new GsonBuilder().create();

    public static void main(String[] args) {

        log.info("starting app");

        Connection conn;
        try {
            conn = DriverManager.getConnection(HOST, MASTER_USERNAME, MASTER_PASSWORD);
            Statement statement = conn.createStatement();

            getStartingDetails(statement);

            //get position data for DC
            FileService fileService = new FileService(FLAG_CODE);
            List<Position> positions = fileService.processFile(FILE_LOCATION);
            int totalPositions = positions.size();
            AtomicInteger count = new AtomicInteger();
            Timestamp starttimestamp = new Timestamp(System.currentTimeMillis());
            log.info("Start: " + starttimestamp);

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

            getEndingDetails(statement);

            conn.close();

            Timestamp endtimestamp = new Timestamp(System.currentTimeMillis());

            log.info("finished: " + endtimestamp);
            log.info("time taken: " + Duration.between(starttimestamp.toInstant(), endtimestamp.toInstant()).toString());

        } catch (SQLException e) {
            log.error("Error with SQL Batch", e);
        }


    }

    private static void getEndingDetails(Statement statement) throws SQLException {
        log.info("Ending count in shipposition table: " + getShipPositionCount(statement));
        log.info("new max shippositionID: " + getMaxShipPositionID(statement));
    }

    private static void getStartingDetails(Statement statement) throws SQLException {
        log.info("Starting count in shipposition table: " + getShipPositionCount(statement));
        log.info("ShipPosition Generator Value: " + getGeneratorValue(statement));

        int maxID = getMaxShipPositionID(statement);
        log.info("max ship position id is: " + maxID + " Current shipPositionID: " + shipPositionID);
        if(maxID > shipPositionID){
            throw new RuntimeException("Need to increase shipPositionID as maxID is " + maxID);
        }
    }

    private static int getGeneratorValue(Statement statement) throws SQLException {
        ResultSet out = statement.executeQuery("SELECT sequence_next_hi_value AS shipPositionTotal from generator where sequence_name='ShipPosition'");
        out.next();
        return out.getInt("shipPositionTotal");
    }

    private static int getMaxShipPositionID(Statement statement) throws SQLException {
        //get starting count
        ResultSet out = statement.executeQuery("SELECT max(id) AS maxID from shipposition");
        out.next();
        return out.getInt("maxID");
    }

    private static int getShipPositionCount(Statement statement) throws SQLException {
        //get starting count
        ResultSet out = statement.executeQuery("SELECT COUNT(*) AS total from shipposition");
        out.next();
        return out.getInt("total");
    }

    private static String constructSQLQuery(Position position) {
        String createTimestamp = String.format("%s %s", position.getCreation_date(), position.getCreation_time());
        String gnssTimestamp = String.format("%s %s", position.getTrail_date(), position.getTrail_time());

        String sql = String.format(
                "insert into shipposition as sp " +
                "(id, aspidentifier, aspreceivetimestamp, asptransmittimestamp, averagespeed, datacenteridentifier, datauserprovideridentifier, heading, mmsi, latitude, longitude, receivetimestamp, shipname, shippositiontype, shipborneequipmentidentifier, shipborneequipmenttimestamp, speed, version, ship_id) " +
                "select " +
                "'%d', '4001', '%s', '%s', '%s', '%s', '%s', '%s', (select mmsi from ship as s where s.imonumber = '%s'), '%s', '%s', '%s', (select imonumber from ship as s where s.imonumber = '%s'), 'PERIODIC_REPORT', (select shipborneequipmentidentifier from ship as s where s.imonumber = '%s'), '%s', '%s', '0', (select id from ship as s where s.imonumber = '%s') " +
                "WHERE NOT EXISTS (SELECT id FROM shipposition WHERE shipborneequipmenttimestamp = '%s');",
                shipPositionID, createTimestamp, createTimestamp, position.getTrail_speed(), DATA_CENTER_IDENTIFIER, position.getDC_ID(), position.getTrail_heading(), position.getI_m_o_number(), position.getTrail_latitude(), position.getTrail_longitude(), createTimestamp, position.getI_m_o_number(), position.getI_m_o_number(), gnssTimestamp, position.getTrail_speed(), position.getI_m_o_number(), gnssTimestamp);
        shipPositionID++;
        return sql;
    }


}
