package org.hypertrace.core.query.service.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class TimeUtil {

    public static long round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.longValue();
    }


    public static long convertSecondsToMinutes(long seconds) {
        double minutes = seconds/(60.0);
        return round(minutes, 0);
    }

}
