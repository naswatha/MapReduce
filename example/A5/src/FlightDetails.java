
/**
 *
 * Created by sujith, karthik on 2/20/16.
 */
public class FlightDetails implements Comparable<FlightDetails> {

    public Boolean type;
    public long scheduledTime;
    public long actualTime;
    public short cancelled;

    public FlightDetails(Boolean type, long scheduledTime, long actualTime, short cancelled) {
        this.type = type;
        this.scheduledTime = scheduledTime;
        this.actualTime = actualTime;
        this.cancelled = cancelled;
    }
	
	public FlightDetails(String details) {
		String[] splitDetails = details.split(",");
		this.type = Boolean.parseBoolean(splitDetails[0]);
		this.scheduledTime = Long.parseLong(splitDetails[1]);
		this.actualTime = Long.parseLong(splitDetails[2]);
		this.cancelled = Short.parseShort(splitDetails[3]);
	}

    public FlightDetails() {

    }

    @Override
    public int compareTo(FlightDetails flightDetails) {
        // compare the 2 objects based on scheduledTime
        long flightScheduledTime = flightDetails.scheduledTime;

        long diff = this.scheduledTime - flightScheduledTime;

        if (diff < 0) return -1;
        if (diff == 0) return 0;
        return 1;
    }

    @Override
    public String toString() {
        return "FlightDetails{" +
                "type=" + type +
                ", scheduledTime=" + scheduledTime +
                ", actualTime=" + actualTime +
                ", cancelled=" + cancelled +
                '}';
    }
}
