package edu.buffalo.cse.ambience.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.buffalo.cse.ambience.parameters.CLI;

public class AMBIENCE_continuous extends AMBIENCE 
{
	
	public AMBIENCE_continuous(CLI cli,Configuration conf)
	{
		super(cli,conf);
	}

	

	@Override
	public boolean start() 
	{
		return false;
	}



	@Override
	public boolean kwii(Job job, String sinkT) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}



	@Override
	public boolean pai(Job job, String sinkT) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean all(Job job) throws IOException, InterruptedException,
			ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean kwiiList(Job job, String sinkT) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}



	@Override
	public boolean skip(Job job, String sinkT) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}



	@Override
	public boolean skipC(Job job, String sinkT) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}



	

	@Override
	public boolean contigency(Job job, String sinkT) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}



	@Override
	public boolean iter(Job job, String sinkT) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return false;
	}
}
