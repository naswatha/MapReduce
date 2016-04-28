import java.io.IOException;
import java.util.List;

import mapreduce.Context;
import mapreduce.DoubleWritable;
import mapreduce.Mapper;
import mapreduce.MrJob;
import mapreduce.Reducer;
import mapreduce.Text;
import mapreduce.Writable;

public class TestMain {
	public static void main(String[] args) {
		MrJob mrjob = new MrJob();
		mrjob.setInputDirPath("s3://mrclassvitek/data");
		mrjob.setOutputDirPath("s3://hua9/output/");
		mrjob.setMapperClass(AirlinePriceMapper.class);
		mrjob.setReducerClass(AirlinePriceReducer.class);
		mrjob.setMapperOutputKey(Text.class);
		mrjob.setMapperOutputValue(DoubleWritable.class);
		mrjob.setReducerOutputKey(Text.class);
		mrjob.setReducerOutputValue(DoubleWritable.class);
		try {
			mrjob.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}