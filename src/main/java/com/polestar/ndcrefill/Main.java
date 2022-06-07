package com.polestar.ndcrefill;

import java.sql.*;
import java.util.List;

public class Main {

    private static final String HOST = "jdbc:postgresql://rds-manager.polestar-testing.local:5432/";
    private static final String MASTER_USERNAME = "test_pan_dc";
    private static final String MASTER_PASSWORD = "Vod2FrovyerUc8";

    private static final String FLAG_CODE = "1108";
    private static int shipPositionID = 3000000;


    public static void main(String[] args) {

        Connection conn;
        try {

            conn = DriverManager.getConnection(HOST, MASTER_USERNAME, MASTER_PASSWORD);
            Statement statement = conn.createStatement();

            //get position data for DC
            FileService fileService = new FileService(FLAG_CODE);
            List<Position> positions = fileService.processFile("src/main/resources/missing_positions-test.csv");

            //iterate through data
            positions.forEach(position -> {
                String insertStatement = constructSQLQuery(position);
                System.out.println(insertStatement);
                //execute
                try {
                    statement.execute(insertStatement);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static String constructSQLQuery(Position position) {
        String createTimestamp = String.format("%s %s", position.getCreation_date(), position.getCreation_time());
        String gnssTimestamp = String.format("%s %s", position.getTrail_date(), position.getTrail_time());
        String dataCenterIdentifier = "3" + position.getDC_ID().substring(1);

        String sql = String.format(
                "insert into shipposition as sp " +
                "(id, aspidentifier, aspreceivetimestamp, asptransmittimestamp, averagespeed, datacenteridentifier, datauserprovideridentifier, heading, mmsi, latitude, longitude, receivetimestamp, shipname, shippositiontype, shipborneequipmentidentifier, shipborneequipmenttimestamp, speed, version, ship_id) " +
                "select " +
                "'%d', '4001', '%s', '%s', '%s', '%s', '%s', '%s', (select mmsi from ship as s where s.imonumber = '%s'), '%s', '%s', '%s', (select imonumber from ship as s where s.imonumber = '%s'), 'PERIODIC_REPORT', (select shipborneequipmentidentifier from ship as s where s.imonumber = '%s'), '%s', '%s', '0', (select id from ship as s where s.imonumber = '%s') " +
                "WHERE NOT EXISTS (SELECT id FROM shipposition WHERE shipborneequipmenttimestamp = '%s');",
                shipPositionID, createTimestamp, createTimestamp, position.getTrail_speed(), dataCenterIdentifier, position.getDC_ID(), position.getTrail_heading(), position.getI_m_o_number(), position.getTrail_latitude(), position.getTrail_longitude(), createTimestamp, position.getI_m_o_number(), position.getI_m_o_number(), gnssTimestamp, position.getTrail_speed(), position.getI_m_o_number(), gnssTimestamp);
        shipPositionID++;
        return sql;
    }


}
