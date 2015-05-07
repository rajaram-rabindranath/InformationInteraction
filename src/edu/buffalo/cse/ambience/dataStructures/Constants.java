package edu.buffalo.cse.ambience.dataStructures;

import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;

public class Constants 
{
	public static final String COMB_SEP ="|"; 
	public static final String COMB_SPLIT="\\|";
	public static final String VAL_SEP="_";
	public static final String VAL_SPLIT="_";
	public static final String REGEX_KEY="\\|trait1";
	public static final Pattern KeyPattern = Pattern.compile(REGEX_KEY);
	public static final String MAP_KEY="map";
	public static final String DELIM_COMMA=",";
	public static final String DELIM_TAB="\t";
	public static final String mapTblQual="map";
	public static final int FLUSH_INTERVAL_DEFAULT=5;
}
