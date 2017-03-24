package ca.dal.csci6405.project.mrjob02;
/**
 * Created by sj on 23/03/17.
 */

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HDPReducer extends Reducer<IntWritable,StatMap,IntWritable,StatMap> {
    public void reduce( IntWritable key, Iterable<StatMap> values, Context context )
            throws IOException, InterruptedException {
        StatMap accumulator = new StatMap();
        for ( Iterator<StatMap> it = values.iterator(); it.hasNext(); accumulator.add(it.next()) ) ;
        context.write(key,accumulator);
    }
}

