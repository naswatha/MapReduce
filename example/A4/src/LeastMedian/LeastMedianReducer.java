package LeastMedian;

import mapreduce.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 *
 * Created by Karthik, Sujith on 2/10/16.
 */
public class LeastMedianReducer extends Reducer{

    @Override
    public void reduce(Writable key, List<Writable> values, Context context) throws IOException, InterruptedException {

        List<Float> prices = new ArrayList<Float>();

        for (Writable v : values) {
        	FloatWritable value = (FloatWritable) v; 
            prices.add(value.getFloat());
        }

        Collections.sort(prices);

        float median;

        // Computing the median

        if (prices.size() % 2 == 0)
            //average of middle 2 elements if the list is even
            median = ((float) prices.get(prices.size() / 2) + (float) prices.get(prices.size() / 2 - 1)) / 2;
        else
            //taking the median when the list is odd
            median = (float) prices.get(prices.size() / 2);

        BigDecimal bd = new BigDecimal(median);

        // rounding the average to two decimal places
        bd = bd.setScale(2,BigDecimal.ROUND_HALF_UP);
        median = bd.floatValue();

        context.writeReducer((Text)key, new FloatWritable(median));

    }

}