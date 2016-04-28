
import java.io.IOException;


import mapreduce.*;

public class TestMain {
	public static void main(String[] args) {
		MrJob mrjob = new MrJob();
		mrjob.setInputDirPath("s3://mrclassvitek/data");
		mrjob.setOutputDirPath("s3://hua9/output/");
		mrjob.setMapperClass(CheapAirlineMapper.class);
		mrjob.setReducerClass(CheapAirlineReducer.class);
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