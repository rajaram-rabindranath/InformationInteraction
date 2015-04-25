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
	VARLIST_FLAG("v"),
	OPS_FLAG("o"),
	INVALID_FLAG("i"),
	REDUCER_CNT_FLAG("r"),
	PRINT_FLAG("p"),
	MODE_FLAG("m"),
	SPLITS_FLAG("s"),
	HELP_FLAG("h"),
	TOP_COMBINATIONS_FLAG("x"),
	TOP_T_FLAG("t"),
	JOBID_FLAG("j"),
	METRIC_ORDER_FLAG("z");
	private String flag;
	private UserArgs(String flag) 
	{
		this.flag = flag;
	}
	public String toString()
	{
		return getFlag();
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
	private static final String DEFAULT_TOP_T="10";
	private static final String DEFAULT_TOP_T_ORDER="top";
	private HashMap<UserArgs,String> params =new HashMap<UserArgs,String>();

	static 
	{
		options.addOption(UserArgs.SRC_FILE_FLAG.getFlag(),true,"Source file");
		options.addOption(UserArgs.VARLIST_FLAG.getFlag(),true,"Combination");
		options.addOption(UserArgs.INVALID_FLAG.getFlag(),true,"Invalid value");
		options.addOption(UserArgs.KWAY_FLAG.getFlag(),true,"K way number");
		options.addOption(UserArgs.OPS_FLAG.getFlag(),true,"Ambience operation");
		options.addOption(UserArgs.REDUCER_CNT_FLAG.getFlag(),true,"# Reducers");
		options.addOption(UserArgs.MODE_FLAG.getFlag(),true,"Mode");
		options.addOption(UserArgs.SPLITS_FLAG.getFlag(),true,"# Splits for Regions");
		options.addOption(UserArgs.HELP_FLAG.getFlag(),true,"help menu");
		options.addOption(UserArgs.JOBID_FLAG.getFlag(),true,"JobID");
		options.addOption(UserArgs.METRIC_ORDER_FLAG.getFlag(),true,"Sort order of metric"); // FIXME need to catch this
		options.addOption(UserArgs.TOP_T_FLAG.getFlag(),true,"Top T values of the metric being tracked");
		options.addOption(UserArgs.TOP_COMBINATIONS_FLAG.getFlag(),true,"Top Combinations");
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
		if(!cmd.hasOption(UserArgs.SRC_FILE_FLAG.getFlag()))
		{
			System.out.println("Source file has not been furnished");
			help();
			return false;
		}
		
		// must have jobID for distributed mode
		if(!cmd.hasOption(UserArgs.JOBID_FLAG.getFlag()))
		{
			System.out.println("Source file has not been furnished");
			help();
			return false;
		}
		
		// validity of invalid value
		if(cmd.hasOption(UserArgs.INVALID_FLAG.getFlag()))
		{
			String s=cmd.getOptionValue(UserArgs.INVALID_FLAG.getFlag());
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
	
	private void setVarList()
	{
		if(cmd.hasOption(UserArgs.VARLIST_FLAG.getFlag()))
		{
			params.put(UserArgs.VARLIST_FLAG,cmd.getOptionValue(UserArgs.VARLIST_FLAG.getFlag()));
		}
	}
	
	/**
	 * Each argument must be wetted here
	 * @return
	 */
	private void setParams()
	{
		setFileName();
		setJobID();
		setMode();
		setSplitsCnt();
		setKway();
		setInvalid();
		setReducerCnt();
		setTcount();
		setTopCombinations();
		setVarList();
		setOperation();
	}
	
	
	public String getTopCombinations()
	{
		return params.get(UserArgs.TOP_COMBINATIONS_FLAG);
	}
	
	/**
	 * 
	 */
	private void help()
	{
		HelpFormatter formater= new HelpFormatter();
		formater.printHelp("hadoop jar ambience.jar ",options);
	}

	
	public String getFileName() 
	{
		return params.get(UserArgs.SRC_FILE_FLAG);
	}

	
	public String getJobID()
	{
		return params.get(UserArgs.JOBID_FLAG);
	}
	
	
	public String getKway() 
	{
		return params.get(UserArgs.KWAY_FLAG);
	}

	
	public String getOperation() 
	{

		return params.get(UserArgs.OPS_FLAG);
	}

	
	public String getInvalid() 
	{
		return params.get(UserArgs.INVALID_FLAG);
	}

	
	public String getReducerCnt() 
	{
		return params.get(UserArgs.REDUCER_CNT_FLAG);
	}

	
	public String getMode() 
	{
		return params.get(UserArgs.MODE_FLAG);
	}

	
	public String getSplitsCnt() 
	{
		return params.get(UserArgs.SPLITS_FLAG);
	}

	public boolean hasVarList()
	{
		return cmd.hasOption(UserArgs.VARLIST_FLAG.toString());
	}
	
	public String getVarList() 
	{
		return params.get(UserArgs.VARLIST_FLAG);
	}

	
	public String getTvalue() 
	{
		return params.get(UserArgs.TOP_T_FLAG);
	}

	@Override
	public String getTopTOrder() 
	{
		return params.get(UserArgs.METRIC_ORDER_FLAG);
	}
	
	private void setFileName() 
	{
		params.put(UserArgs.SRC_FILE_FLAG,cmd.getOptionValue(UserArgs.SRC_FILE_FLAG.getFlag()));
	}

	private void setMetricOrder()
	{
		if(cmd.hasOption(UserArgs.METRIC_ORDER_FLAG.getFlag()))
		{
			params.put(UserArgs.METRIC_ORDER_FLAG,cmd.getOptionValue(UserArgs.METRIC_ORDER_FLAG.getFlag()));
		}
		else
		{
			params.put(UserArgs.METRIC_ORDER_FLAG,DEFAULT_TOP_T_ORDER);
		}
	}
	
	private void setKway() 
	{
		if(cmd.hasOption(UserArgs.KWAY_FLAG.getFlag()))
		{
			String s=cmd.getOptionValue(UserArgs.KWAY_FLAG.getFlag());
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
	}

	
	private void setOperation() 
	{
		if(cmd.hasOption(UserArgs.OPS_FLAG.getFlag()))
		{
			params.put(UserArgs.OPS_FLAG,cmd.getOptionValue(UserArgs.OPS_FLAG.getFlag()));
		}
		
	}

	
	private void setInvalid() 
	{
		if(cmd.hasOption(UserArgs.INVALID_FLAG.getFlag()))
		{
			params.put(UserArgs.INVALID_FLAG,cmd.getOptionValue(UserArgs.INVALID_FLAG.getFlag()));
		}
	}

	
	private void setReducerCnt() 
	{
		if(cmd.hasOption(UserArgs.REDUCER_CNT_FLAG.getFlag()))
		{
			// check if reducer count is a valid number 
			String reducers  = cmd.getOptionValue(UserArgs.REDUCER_CNT_FLAG.getFlag());
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

	
	private void setMode() 
	{
		if(cmd.hasOption(UserArgs.MODE_FLAG.getFlag()))
		{
			params.put(UserArgs.MODE_FLAG,cmd.getOptionValue(UserArgs.MODE_FLAG.getFlag()));
		}
	}

	
	private void setSplitsCnt() 
	{
		if(cmd.hasOption(UserArgs.SPLITS_FLAG.getFlag()))
		{
			// would like to have as many regions as nodes in CCR
			String splits = cmd.getOptionValue(UserArgs.SPLITS_FLAG.getFlag());
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
	}

	private void setTopCombinations()
	{
		
	}
	
	private void setTcount() 
	{
		if(cmd.hasOption(UserArgs.TOP_T_FLAG.getFlag()))
		{
			String topT  = cmd.getOptionValue(UserArgs.TOP_T_FLAG.getFlag());
			try
			{
				Integer.valueOf(topT);
				params.put(UserArgs.TOP_T_FLAG,topT);
			}
			catch(NumberFormatException nex)
			{
				params.put(UserArgs.TOP_T_FLAG,DEFAULT_TOP_T);
				System.out.println("The topT count given by user is invalid setting it to default");
			}
			setMetricOrder();
		}
	}
	
	private void setJobID()
	{
		params.put(UserArgs.JOBID_FLAG,cmd.getOptionValue(UserArgs.JOBID_FLAG.getFlag()));
	}
}
