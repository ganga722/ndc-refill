package com.polestar.ndcrefill;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FileService {

    private final String flagCode;
    int count = 0;

    public FileService(String flagCode) {
        this.flagCode = flagCode;
    }

    public List<Position> processFile(String fileName) {
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
            System.out.println(count);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return positions;
    }

    private Position processLine(String line) {
        String[] splitted = line.split(",");

        if (splitted[9].equals(flagCode)) {
            count ++;
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
            return position;
        }
        return null;
    }

}
