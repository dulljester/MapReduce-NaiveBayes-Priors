package ca.dal.csci6405.project.mrjob02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;

public class HDPMapper extends Mapper<LongWritable,Text,IntWritable,StatMap> {
    private final IntWritable ONE = new IntWritable(1);
    public void map( LongWritable key, Text value, Context con ) throws IOException, InterruptedException {
        String txt = value.toString();
        for ( String line : txt.split("\n") ) {
            Scanner scan = new Scanner(line);
            long k = scan.nextLong();
            int freq = scan.nextInt(), cl = (int)(k>>(MyUtils.M*MyUtils.WIDTH));
            if ( 0 == (k & MyUtils.MASK(MyUtils.WIDTH*MyUtils.M)) )
                con.write(new IntWritable(-cl),new StatMap(1L,freq)); // class marginals
            else
                con.write(new IntWritable(cl),new StatMap(k,freq)); // only term or (class x term) marginals
        }
    }
}