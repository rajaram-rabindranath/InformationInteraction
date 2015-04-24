package edu.buffalo.cse.ambience;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.buffalo.cse.ambience.core.AMBIENCE_discrete;
import edu.buffalo.cse.ambience.parameters.CLI;

public class Runner extends Configured implements Tool 
{
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Runner(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		CLI cli = CLI.getInstance(args);
		if(cli == null) return 1;
		AMBIENCE_discrete d = new AMBIENCE_discrete(cli, this.getConf());
		if(!d.start()) return 1;
		return 0;
	}
}