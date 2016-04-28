import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.Context;
import mapreduce.DoubleWritable;
import mapreduce.Reducer;
import mapreduce.Text;
import mapreduce.Writable;

public class AirlinePriceReducer extends Reducer{
    protected void reduce(Writable key, List<Writable> values, Context context) throws IOException, InterruptedException {
    	int count =0;
    	Double sum = 0.0D;
    	ArrayList<Double> price = new ArrayList<Double>();
    	for(Writable value : values) {
    		count ++;
    		DoubleWritable f = (DoubleWritable) value;
    		sum+= f.getDouble();
    		price.add(f.getDouble());
    	}
    	Collections.sort(price);
    	context.writeReducer((Text)key, new DoubleWritable(price.size()/2 == 0 ? price.get(price.size()/2) + price.get(price.size()/2+1) : price.get(price.size()/2) ));
    	context.writeReducer((Text)key, new DoubleWritable(sum/count));
    }
}