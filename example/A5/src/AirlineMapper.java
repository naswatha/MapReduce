/**
 *
 * Created by sujith, karthik on 2/20/16.
 */

import mapreduce.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.text.StrTokenizer;

public class AirlineMapper extends Mapper {
    @Override
    public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {

        String line = ((Text)value).get();
        StrTokenizer t = new StrTokenizer(line, ',','"');

        t.setIgnoreEmptyTokens(false);
        String[] columns = t.getTokenArray();

        if (sanityTest(columns) && columns.length == 110 && Double.parseDouble(columns[109]) < 100000) {

            /* FROM THE DATASET */
            String carrierCode = columns[8];
            String year = columns[0];

            String origin = columns[14];
            String destination = columns[23];

            String flightDate = columns[5];

            // Key would be carrier code and year
            String carrierAndYear = carrierCode+","+year;

            int scheduledArrivalTime = Integer.parseInt(columns[40]);
            int actualArrivalTime = Integer.parseInt(columns[41]);

            int scheduledDepartureTime = Integer.parseInt(columns[29]);
            int actualDepartureTime = Integer.parseInt(columns[30]);

            int scheduledElapsedTime = Integer.parseInt(columns[50]);

            String cancelled = columns[47];

            //////////////////////////////////////////////////////////

            /* Converting the values from the dataset to milliseconds and calculating the diff */

            long scheduledDepartureInMs = getMilliSecsFromTime(scheduledDepartureTime);
            long actualDepartureInMs = getMilliSecsFromTime(actualDepartureTime);

            long scheduledArrivalInMs = getMilliSecsFromTime(scheduledArrivalTime);
            long actualArrivalInMs = getMilliSecsFromTime(actualArrivalTime);

            long scheduledElapsedInMs = scheduledElapsedTime * 60000;
            long diff = scheduledArrivalInMs - scheduledDepartureInMs - scheduledElapsedInMs;
            Boolean dayRollOver = false;

            // To check if the difference is greater than 6 hours in milliseconds
            // Pointing to the next day
            long SIX_HOURS = 360*60000;

            if (diff <= (-1 * SIX_HOURS)) {
                dayRollOver = true;
            }

            ////////////////////////////////////////////////////////////////////

            try {
                //calculating epoch times
                long flightDateEpoch = getEpochTime(flightDate);

                long scheduledDepartureInEpoch = flightDateEpoch + scheduledDepartureInMs;
                long actualDepartureInEpoch = flightDateEpoch + actualDepartureInMs;

                long scheduledArrivalInEpoch = flightDateEpoch + scheduledArrivalInMs;
                long actualArrivalInEpoch = flightDateEpoch + actualArrivalInMs;

                if (dayRollOver) {
                    long ONE_DAY = 24*60*60000;
                    scheduledArrivalInEpoch += ONE_DAY;
                    actualArrivalInEpoch += ONE_DAY;
                }

                Short cancelFlag = Short.parseShort(cancelled);

                // writing to context twice. Once as the first flight and then as the second flight
                // second flight
                String flightDetails1 = "false"+","+String.valueOf(scheduledDepartureInEpoch)+","+
                String.valueOf(actualDepartureInEpoch)+","+String.valueOf(cancelFlag);
                context.writeMapper(new Text(carrierAndYear+","+origin.trim()), new Text(flightDetails1));

                //first flight
                String flightDetails2 = "true"+","+String.valueOf(scheduledArrivalInEpoch)+","
                +String.valueOf(actualArrivalInEpoch)+","+String.valueOf(cancelFlag);
                context.writeMapper(new Text(carrierAndYear+","+destination.trim()), new Text(flightDetails2));

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    private static long getMilliSecsFromTime(int time) {
        int hoursPart = time / 100 ;
        int minutesPart = time % 100;

        long finalTime = (hoursPart * 60) + minutesPart;

        return finalTime * 60000;

    }

    private static long getEpochTime(String flightDate) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        Date date = formatter.parse(flightDate);

        return date.getTime();

    }


    private static boolean sanityTest(String[] line) {

        try {
            int crsArrTime = Integer.parseInt(line[40]);
            int actArrTime = Integer.parseInt(line[41]);
            //converting to mins
            crsArrTime = (crsArrTime % 100) + (crsArrTime / 100) * 60;
            int crsDepTime = Integer.parseInt(line[29]);
            int actDepTime = Integer.parseInt(line[30]);
            //converting to mins
            crsDepTime = (crsDepTime % 100) + (crsDepTime / 100) * 60;
            //check if CRS arrival and departure times are not 0
            if (crsArrTime != 0 && crsDepTime != 0) {
                int timezone = crsArrTime - crsDepTime - Integer.parseInt(line[50]);
                //check if timezone is a multiple of 60
                if (timezone % 60 == 0) {
                    if (Integer.parseInt(line[11]) > 0              //ORIGIN_AIRPORT_ID
                            && Integer.parseInt(line[12]) > 0       //ORIGIN_AIRPORT_SEQ_ID
                            && Integer.parseInt(line[13]) > 0       //ORIGIN_CITY_MARKET_ID
                            && Integer.parseInt(line[17]) > 0       //ORIGIN_STATE_FIPS
                            && Integer.parseInt(line[19]) > 0       //ORIGIN_WAC
                            && Integer.parseInt(line[20]) > 0       //DEST_AIRPORT_ID
                            && Integer.parseInt(line[21]) > 0       //DEST_AIRPORT_SEQ_ID
                            && Integer.parseInt(line[22]) > 0       //DEST_CITY_MARKET_ID
                            && Integer.parseInt(line[26]) > 0       //DEST_STATE_FIPS
                            && Integer.parseInt(line[28]) > 0) {    //DEST_WAC

                        if (!line[14].isEmpty()                     //ORIGIN
                                && !line[15].isEmpty()              //ORIGIN_CITY_NAME
                                && !line[16].isEmpty()              //ORIGIN_STATE_ABR
                                && !line[18].isEmpty()              //ORIGIN_STATE_NM
                                && !line[23].isEmpty()              //DEST
                                && !line[24].isEmpty()              //DEST_CITY_NAME
                                && !line[25].isEmpty()              //DEST_STATE_ABR
                                && !line[27].isEmpty()) {           //DEST_STATE_NM
                            if (Boolean.parseBoolean(line[47])) {                   //CANCELLED
                                int arrTime = Integer.parseInt(line[41]);           //ARR_TIME
                                arrTime = (arrTime % 100) + (arrTime / 100) * 60;
                                int depTime = Integer.parseInt(line[30]);           //DEP_TIME
                                depTime = (depTime % 100) + (depTime / 100) * 60;
                                int elapTime = Integer.parseInt(line[51]);          //ACTUAL_ELAPSED_TIME
                                int time = arrTime - depTime - elapTime
                                        - timezone;
                                if (time == 0) {
                                    double arrDelay = Double.parseDouble(line[42]); //ARR_DELAY
                                    double arrDelayMin = Double
                                            .parseDouble(line[43]);                 //ARR_DELAY_NEW
                                    if (arrDelay < 0) {
                                        return arrDelayMin == 0;
                                    } else if (arrDelay > 0) {
                                        return arrDelay == arrDelayMin;
                                    }

                                    if (arrDelayMin >= 15) {
                                        //ARR_DEL15
                                        return Double.parseDouble(line[44]) == 1;
                                    }
                                }
                            } else
                                return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

}
