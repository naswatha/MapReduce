package LeastMedian;

import java.io.IOException;


import mapreduce.*;

public class TestMain {
	public static void main(String[] args) {
		MrJob mrjob = new MrJob();
		mrjob.setInputDirPath("s3://mrclassvitek/data");
		mrjob.setOutputDirPath("s3://hua9/output/");
		mrjob.setMapperClass(LeastMedianMapper.class);
		mrjob.setReducerClass(LeastMedianReducer.class);
		mrjob.setMapperOutputKey(Text.class);
		mrjob.setMapperOutputValue(FloatWritable.class);
		mrjob.setReducerOutputKey(Text.class);
		mrjob.setReducerOutputValue(FloatWritable.class);
		mrjob.setGlobalVal(args[0]);
		try {
			mrjob.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}