package edu.buffalo.cse.ambience.dataStructures;

import java.util.regex.Pattern;

public class Constants 
{
	public static final String COMB_SEP ="|"; 
	public static final String COMB_SPLIT="\\|";
	public static final String VAL_SEP="_";
	public static final String VAL_SPLIT="_";
	public static final String REGEX_KEY="\\|trait1";
	public static final Pattern KeyPattern = Pattern.compile(REGEX_KEY);
	public static final String MAP_KEY="map";
}
