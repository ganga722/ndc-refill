package com.polestar.ndcrefill;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class FileService {

    private final String flagCode;

    String DATE_REGEX = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
    String TIME_REGEX = "[0-9]{2}:[0-9]{2}:[0-9]{2}";

    String SIMPLE_IMO_REGEX = "[0-9]{7}";
    int count = 0;

    public FileService(String flagCode) {
        this.flagCode = flagCode;
    }

    public List<Position> processFile(String fileName) {

        log.info("starting to process file");
        List<Position> positions = new ArrayList<>();
        Path path = Paths.get(fileName);

        try {
            try (Stream<String> stream = Files.lines(path)) {
                stream.forEach(line -> {
                    Position position = processLine(line);
                    if (position != null){
                        positions.add(position);
                    }
                });
            }

        } catch (IOException e) {
            log.error("error reading file");
            e.printStackTrace();
        }

        log.info("Positions count: " + count);

        return positions;
    }

    private Position processLine(String line) {
        String[] splitted = line.split(",");
        try {
            if (flagCode.equals(splitted[9])) {
                count++;
                Position position = new Position();
                position.setCreation_date(splitted[0]);
                position.setCreation_time(splitted[1]);
                position.setTrail_date(splitted[2]);
                position.setTrail_time(splitted[3]);
                position.setTrail_latitude(splitted[4]);
                position.setTrail_longitude(splitted[5]);
                position.setTrail_speed(splitted[6]);
                position.setTrail_heading(splitted[7]);
                position.setI_m_o_number(splitted[8]);
                position.setDC_ID(splitted[9]);
                
                validateFields(position);
                
                return position;
            }
        } catch (ArrayIndexOutOfBoundsException | InvalidPropertiesFormatException e){
            log.error(e.getLocalizedMessage());
            log.error("line skipped: " + line);
        }
        return null;
    }



    private void validateFields(Position position) throws InvalidPropertiesFormatException {
        dateCheck(position.getCreation_date());
        dateCheck(position.getTrail_date());
        timeCheck(position.getCreation_time());
        timeCheck(position.getTrail_time());
        imoCheck(position.getI_m_o_number());
    }

    private void imoCheck(String i_m_o_number) throws InvalidPropertiesFormatException {
        if(!i_m_o_number.matches(SIMPLE_IMO_REGEX)){
            throw new InvalidPropertiesFormatException("bad i_m_o_number " + i_m_o_number);
        }
    }

    private void timeCheck(String time) throws InvalidPropertiesFormatException {
        if(!time.matches(TIME_REGEX)){
            throw new InvalidPropertiesFormatException("bad time " + time);
        }
    }

    private void dateCheck(String date) throws InvalidPropertiesFormatException {
        if(!date.matches(DATE_REGEX)){
            throw new InvalidPropertiesFormatException("bad date: " + date);
        }
    }

}
