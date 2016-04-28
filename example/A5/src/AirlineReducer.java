/**
 * Created by sujith,karthik on 2/20/16.
 *
 */
import mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AirlineReducer extends Reducer {

    @Override
    public void reduce(Writable key, List<Writable> values, Context context) throws IOException, InterruptedException {

        long connections = 0L;
        long missedConnections = 0L;

        List<FlightDetails> flightDetailsList = new ArrayList<>();

        // creating a array list of all the flight details. Since we want to iterate 2 times.
        for (Writable fDetails :values) {
			String details = ((Text)fDetails).get();
            FlightDetails fDetailsNew = new FlightDetails(details);
            flightDetailsList.add(fDetailsNew);
        }

        // sort the list based on scheduledTime
        Collections.sort(flightDetailsList);

        int numberOfFlights = flightDetailsList.size();

        for (FlightDetails f : flightDetailsList) {

            int otherIndex = flightDetailsList.indexOf(f);

            // skip this iteration if its not the first flight
            if(!f.type)
                continue;

            //check if flight index is less than number of flights. Also only check for flights within the next 6 hours
            while(otherIndex<numberOfFlights && ((flightDetailsList.get(otherIndex).scheduledTime - f.scheduledTime) <= (6*60*60000))) {

                // check if other flight is the second flight
                if (!flightDetailsList.get(otherIndex).type) {

                    //calculate scheduled layover between the flights
                    long layOver = (flightDetailsList.get(otherIndex).scheduledTime - f.scheduledTime);
                    //check if scheduled layover is > 30 mins
                    if (layOver >= (30 * 60000)) {
                        // this is a connecting flight
                        connections += 1;
                        //calculate the actual layover between the flights
                        long actualLayOver = flightDetailsList.get(otherIndex).actualTime - f.actualTime;
                        //check if the first flight is cancelled or if the actual layover is less than 30 mins
                        if (f.cancelled == 1 || actualLayOver < (30 * 60000))
                            // this is a missed connection
                            missedConnections += 1;
                    }
                }

                otherIndex+=1;
            }
        }
        String counts = String.valueOf(connections) + "," + String.valueOf(missedConnections);
        if(key == null)
        	return;
        context.writeReducer((Text)key,new Text(counts));

    }

}
