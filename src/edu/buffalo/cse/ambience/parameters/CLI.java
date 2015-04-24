package edu.buffalo.cse.ambience.parameters;

import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

enum UserArgs 
{
	SRC_FILE_FLAG("f"),
	KWAY_FLAG("k"),
	COMB_FLAG("c"),
	OPS_FLAG("o"),
	INVALID_FLAG("i"),
	REDUCER_CNT_FLAG("r"),
	PRINT_FLAG("p"),
	MODE_FLAG("m"),
	SPLITS_FLAG("s"),
	HELP_FLAG("h"),
	JOBID_FLAG("j");
	
	
	private String flag;
	
	private UserArgs(String flag) 
	{
		this.flag = flag;
	}
	
	public String toString()
	{
		return flag;
	}
	
	public String getFlag()
	{
		return flag;
	}
}



public class CLI implements Arguments
{
	private static Options options = new Options();
	private String[] args= null;
	private CommandLine cmd=null;
	private static final String DEFAULT_K_WAY="3";
	private static final String DEFAULT_REDUCERS="2";
	private static final String DEFAULT_SPLITS="1";
	private HashMap<UserArgs,String> params =new HashMap<UserArgs,String>();

	static 
	{
		options.addOption(UserArgs.SRC_FILE_FLAG.toString(),true,"Source file");
		options.addOption(UserArgs.COMB_FLAG.toString(),true,"Combination");
		options.addOption(UserArgs.INVALID_FLAG.toString(),true,"Invalid value");
		options.addOption(UserArgs.KWAY_FLAG.toString(),true,"K way number");
		options.addOption(UserArgs.OPS_FLAG.toString(),true,"Ambience operation");
		options.addOption(UserArgs.REDUCER_CNT_FLAG.toString(),true,"# Reducers");
		options.addOption(UserArgs.MODE_FLAG.toString(),true,"Mode");
		options.addOption(UserArgs.SPLITS_FLAG.toString(),true,"# Splits for Regions");
		options.addOption(UserArgs.HELP_FLAG.toString(),true,"help menu");
		options.addOption(UserArgs.JOBID_FLAG.toString(),true,"JobID");
	}
	
	/**
	 * 
	 * @param args
	 */
	private CLI(String args[])
	{
		this.args=args;
	}
	
	/**
	 * 
	 * @param args
	 * @return
	 */
	public static CLI getInstance(String[] args)
	{
		CLI instance = new CLI(args);
		if(!instance.parseArguments()) return null;
		if(!instance.isArgsValid()) return null;
		instance.setParams();
		return instance;
	}
	
	
	/**
	 * 
	 */
	private boolean parseArguments()
	{
		CommandLineParser parser= new BasicParser();
		try 
		{
			cmd = parser.parse(options,args);
		} 
		catch (ParseException e) 
		{
			e.printStackTrace();
			help();
			return false;
		}
		return true;
	}
	
	/**
	 * 
	 * @return
	 */
	private boolean isArgsValid()
	{
		/**
		 * error checking --- 
		 * 1. need source file
		 * 2. invalid value must be legit double
		 * 3. k way must be legit value
		 */
		if(!cmd.hasOption(UserArgs.SRC_FILE_FLAG.toString()))
		{
			System.out.println("Source file has not been furnished");
			help();
			return false;
		}
		
		// must have jobID for distributed mode
		if(!cmd.hasOption(UserArgs.JOBID_FLAG.toString()))
		{
			System.out.println("Source file has not been furnished");
			help();
			return false;
		}
		
		if(cmd.hasOption(UserArgs.INVALID_FLAG.toString()))
		{
			String s=cmd.getOptionValue(UserArgs.INVALID_FLAG.toString());
			try
			{
				Double.parseDouble(s);
			}
			catch(NumberFormatException nex)
			{
				System.out.println("\"Invalid value\" not a valid number");
				help();
				return false;
			}
		}
		return true;
	}
	
	
	public boolean hasCombFlag()
	{
		return cmd.hasOption(UserArgs.COMB_FLAG.toString());
	}
	
	
	
	
	/**
	 * 
	 * @return
	 */
	private void setParams()
	{
		params = new HashMap<UserArgs,String>();
		/**
		 * Each argument must be vetted here
		 */
		// SRC FILE
		params.put(UserArgs.SRC_FILE_FLAG,cmd.getOptionValue(UserArgs.SRC_FILE_FLAG.toString()));
		
		// job id
		params.put(UserArgs.JOBID_FLAG,cmd.getOptionValue(UserArgs.JOBID_FLAG.toString()));
		
		// mode of operations
		if(cmd.hasOption(UserArgs.MODE_FLAG.toString()))
		{
			params.put(UserArgs.MODE_FLAG,cmd.getOptionValue(UserArgs.MODE_FLAG.toString()));
		}
		
		// # regions
		if(cmd.hasOption(UserArgs.SPLITS_FLAG.toString()))
		{
			// would like to have as many regions as nodes in CCR
			String splits = cmd.getOptionValue(UserArgs.SPLITS_FLAG.toString());
			try
			{
				Integer.valueOf(splits);
			}
			catch(NumberFormatException nex)
			{
				nex.printStackTrace();
				splits=DEFAULT_SPLITS;
			}
			params.put(UserArgs.SPLITS_FLAG,splits);
		}
		else
		{
			params.put(UserArgs.SPLITS_FLAG,DEFAULT_SPLITS);
		}
		
		// KWAY
		if(cmd.hasOption(UserArgs.KWAY_FLAG.toString()))
		{
			String s=cmd.getOptionValue(UserArgs.KWAY_FLAG.toString());
			try
			{
				Double.parseDouble(s);
			}
			catch(NumberFormatException nex)
			{
				System.out.println("\"K way\" not a valid number");
				s=DEFAULT_K_WAY;
			}
			params.put(UserArgs.KWAY_FLAG,s);
		}
		else
		{
			params.put(UserArgs.KWAY_FLAG,DEFAULT_K_WAY);
		}
		
		// INVALID VALUE
		if(cmd.hasOption(UserArgs.INVALID_FLAG.toString()))
		{
			params.put(UserArgs.INVALID_FLAG,cmd.getOptionValue(UserArgs.INVALID_FLAG.toString()));
		}
		
		// COMB COLUMNS
		if(cmd.hasOption(UserArgs.COMB_FLAG.toString()))
		{
			params.put(UserArgs.COMB_FLAG,cmd.getOptionValue(UserArgs.COMB_FLAG.toString()));
		}
		
		// OPERATION
		if(cmd.hasOption(UserArgs.OPS_FLAG.toString()))
		{
			params.put(UserArgs.OPS_FLAG,cmd.getOptionValue(UserArgs.OPS_FLAG.toString()));
		}
		
		// # of Reducers
		if(cmd.hasOption(UserArgs.REDUCER_CNT_FLAG.toString()))
		{
			// check if reducer count is a valid number 
			String reducers  = cmd.getOptionValue(UserArgs.REDUCER_CNT_FLAG.toString());
			try
			{
				Integer.valueOf(reducers); // check if the reducer count is a valid value
				params.put(UserArgs.REDUCER_CNT_FLAG,reducers);
			}
			catch(NumberFormatException nex)
			{
				nex.printStackTrace();
				params.put(UserArgs.REDUCER_CNT_FLAG,DEFAULT_REDUCERS);
				System.out.println("The reducer count given by user is invalid");
			}
		}
		else
		{
			params.put(UserArgs.REDUCER_CNT_FLAG,DEFAULT_REDUCERS);
		}
	}
	
	/**
	 * 
	 */
	private void help()
	{
		HelpFormatter formater= new HelpFormatter();
		formater.printHelp("hadoop jar ambience.jar edu.buffalo.cse.ambience.Runner",options);
	}

	@Override
	public String getFileName() 
	{
		return params.get(UserArgs.SRC_FILE_FLAG);
	}

	
	public String getJobID()
	{
		return params.get(UserArgs.JOBID_FLAG);
	}
	
	@Override
	public String getKway() 
	{
		return params.get(UserArgs.KWAY_FLAG);
	}

	@Override
	public String getOperation() 
	{

		return params.get(UserArgs.OPS_FLAG);
	}

	@Override
	public String getInvalid() 
	{
		return params.get(UserArgs.INVALID_FLAG);
	}

	@Override
	public String getReducerCnt() 
	{
		return params.get(UserArgs.REDUCER_CNT_FLAG);
	}

	@Override
	public String getMode() 
	{
		return params.get(UserArgs.MODE_FLAG);
	}

	@Override
	public String getSplitsCnt() 
	{
		return params.get(UserArgs.SPLITS_FLAG);
	}

	@Override
	public String getColFilter() 
	{
		return params.get(UserArgs.COMB_FLAG);
	}
}
