import java.io.IOException;

import mapreduce.Context;
import mapreduce.DoubleWritable;
import mapreduce.Mapper;
import mapreduce.Text;
import mapreduce.Writable;

public class AirlinePriceMapper extends Mapper{
	    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
	    	AirlineParser FParser = new AirlineParser();
	    	if (!FParser.map(value.getString())){
	    		return;
	    	}
	    	context.writeMapper(new Text(FParser.Carrier), new DoubleWritable(FParser.Price));
	}
}