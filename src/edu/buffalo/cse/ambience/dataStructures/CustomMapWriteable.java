package edu.buffalo.cse.ambience.dataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * https://www.chrisstucchio.com/blog/2011/mapwritable_sometimes_a_performance_hog.html
 * copyrights  --- CHRIS STUCCHIO
 */

public class CustomMapWriteable extends MapWritable implements Writable
{

	@Override
	public void readFields(DataInput arg0) throws IOException 
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException 
	{
		// TODO Auto-generated method stub
		
	}
}
