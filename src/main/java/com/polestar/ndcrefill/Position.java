package com.polestar.ndcrefill;

import lombok.Data;

@Data
public class Position {

    private String creation_date;
    private String creation_time;
    private String trail_date;
    private String trail_time;
    private String trail_latitude;
    private String trail_longitude;
    private String trail_speed;
    private String trail_heading;
    private String i_m_o_number;
    private String DC_ID;

}
