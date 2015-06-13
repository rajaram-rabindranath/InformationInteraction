package edu.buffalo.cse.ambience.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import cern.colt.Arrays;
import edu.buffalo.cse.ambience.HBase.Combiners.C_entropy;
import edu.buffalo.cse.ambience.HBase.Combiners.C_kwii;
import edu.buffalo.cse.ambience.HBase.Combiners.C_kwiiList;
import edu.buffalo.cse.ambience.HBase.Combiners.C_pai;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_contingency;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_kwii;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_kwiiList;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai_cumulative_skipper;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai_higherOrder_cummulative;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai_noBlackList;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai_pairwise;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai_periodic;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai_rush;
import edu.buffalo.cse.ambience.HBase.MR.Mappers.M_pai_skipper;
import edu.buffalo.cse.ambience.HBase.MR.Reducers.R_contingency;
import edu.buffalo.cse.ambience.HBase.MR.Reducers.R_kwii;
import edu.buffalo.cse.ambience.HBase.MR.Reducers.R_kwiiList;
import edu.buffalo.cse.ambience.HBase.MR.Reducers.R_pai;
import edu.buffalo.cse.ambience.HBase.MR.Reducers.R_pai2;
import edu.buffalo.cse.ambience.HBase.MR.Reducers.R_pai_cont;
import edu.buffalo.cse.ambience.HBase.MR.Reducers.R_pai_top;
import edu.buffalo.cse.ambience.parameters.CLI;

public class AMBIENCE_discrete extends AMBIENCE
{
	
	public AMBIENCE_discrete(CLI cli,Configuration conf)
	{
		super(cli,conf);
	}
	
	
	@Override
	public boolean start() 
	{
		if(!bootup()) return false;
		return executeJob();
	}
	
	@Override
	public boolean kwii(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException 
	{
		job.setJarByClass(this.getClass());
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
        job.setMapperClass(M_kwii.class);
        //job.setCombinerClass(C_kwii.class);
        job.setReducerClass(R_kwii.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(srcTable,s, M_kwii.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
        return false;
	}
	
	@Override
	public boolean pai(Job job, String sinkT) throws IOException,	InterruptedException, ClassNotFoundException 
	{
		job.setJarByClass(this.getClass());
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
		job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setMapperClass(M_pai.class);
        //job.setCombinerClass(C_pai.class);
        job.setReducerClass(R_pai.class);
		TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(srcTable,s,M_pai.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
        return false;
	}
	
	@Override
	public boolean all(Job job) throws IOException, InterruptedException,ClassNotFoundException 
	{
		/*job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setMapperClass(M_all.class);
        job.setCombinerClass(C_all.class);
        job.setReducerClass(R_all.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(AMBIENCE_tables.source.getName(),s, M_all.class, Text.class,Text.class,job);
        job.waitForCompletion(true);*/
		return false;
	}
	
	@Override
	public boolean kwiiList(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException 
	{
		job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setMapperClass(M_kwii.class);
        job.setCombinerClass(C_kwii.class);
        job.setReducerClass(R_kwii.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(AMBIENCE_tables.source.getName(),s, M_kwii.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
        return false;
	}

	@Override
	public boolean skip(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException 
	{
		job.setJarByClass(this.getClass());;
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
		job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setMapperClass(M_pai_skipper.class);
        job.setCombinerClass(C_pai.class);
        job.setReducerClass(R_pai.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(srcTable,s, M_pai_skipper.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
        return false;
		
	}


	@Override
	public boolean skipC(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException 
	{
		job.setJarByClass(this.getClass());;
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
		job.setOutputFormatClass(MultiTableOutputFormat.class);
       // job.setMapperClass(M_pai_rush.class);
        job.setMapperClass(M_pai_periodic.class);
        //job.setCombinerClass(C_pai.class); 
        job.setReducerClass(R_pai.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        //TableMapReduceUtil.initTableMapperJob(srcTable,s, M_pai_rush.class, Text.class,Text.class,job);
        TableMapReduceUtil.initTableMapperJob(srcTable,s, M_pai_periodic.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
        return false;
	}


	@Override
	public boolean iter(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException 
	{
		/**
		 * # of iterations is a very important parameter
		 * for this job
		 */
		job.setJarByClass(this.getClass());;
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setMapperClass(M_pai_higherOrder_cummulative.class);
		job.setReducerClass(R_pai_top.class);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration());
		TableMapReduceUtil.initTableMapperJob(srcTable,s, M_pai_higherOrder_cummulative.class, Text.class,Text.class,job);
		job.waitForCompletion(true);
		return false;
	}


	@Override
	public boolean contigency(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException 
	{
		job.setJarByClass(this.getClass());
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
        job.setMapperClass(M_contingency.class);
        job.setReducerClass(R_contingency.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(srcTable,s, M_contingency.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
        return false;
	}


	@Override
	public boolean ambi(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException
	{
		job.setJarByClass(this.getClass());
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
        job.setMapperClass(M_pai.class);
        job.setReducerClass(R_pai_top.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(srcTable,s, M_pai.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
		return false;
	}


	@Override
	public boolean pairwise_pai(Job job, String sinkT) throws IOException,InterruptedException, ClassNotFoundException
	{
		job.setJarByClass(this.getClass());
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		String srcTable=AMBIENCE_tables.source.getName()+cli.getJobID();
        job.setMapperClass(M_pai_pairwise.class);
        job.setReducerClass(R_pai_top.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        TableMapReduceUtil.initTableMapperJob(srcTable,s, M_pai_pairwise.class, Text.class,Text.class,job);
        job.waitForCompletion(true);
		return false;
	}
}