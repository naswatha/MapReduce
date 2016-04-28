import java.io.IOException;

import mapreduce.*;

/**
 * Author(s): Karthik, Sujith
 *
 */


public class TestMain {
    public static void main(String[] args) throws Exception {

		MrJob mrjob = new MrJob();
		mrjob.setInputDirPath("s3://mrclassvitek/a6history");
		mrjob.setOutputDirPath("s3://hua9/output/");
		mrjob.setMapperClass(AirlineMapper.class);
		mrjob.setReducerClass(AirlineReducer.class);
		mrjob.setMapperOutputKey(Text.class);
		mrjob.setMapperOutputValue(Text.class);
		mrjob.setReducerOutputKey(Text.class);
		mrjob.setReducerOutputValue(Text.class);
		try {
			mrjob.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

