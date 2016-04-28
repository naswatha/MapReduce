import org.apache.commons.math3.stat.regression.SimpleRegression;
import java.io.IOException;
import java.util.List;

import mapreduce.*;
/**
 *
 * Created by sujith, karthik on 2/10/16.
 */
public class CheapAirlineReducer extends Reducer {
    @Override
    protected void reduce(Writable key, List<Writable> values, Context context) throws IOException, InterruptedException {

        SimpleRegression regression = new SimpleRegression();

        for(Writable v: values){

        	Text value = (Text) v;
            String[] valueArr = value.getString().split("\\|");

            String elapsedTimeString = valueArr[0];
            String priceString = valueArr[1];

            Double elpasedTime = Double.parseDouble(elapsedTimeString);
            Double price = Double.parseDouble(priceString);

            // Adding each data pair to the linear regression computation
            regression.addData(elpasedTime, price);
        }

        // Key remains unchanged, value is the intercept and slope obtained from the linear regression
        context.writeReducer((Text)key, new Text(String.valueOf(regression.getIntercept())+","+String.valueOf(regression.getSlope())));

    }
}