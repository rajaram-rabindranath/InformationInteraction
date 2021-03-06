package edu.buffalo.cse.ambience.core;

import java.io.IOException;
import java.util.ArrayList;

import orderly.Order;
import edu.buffalo.cse.ambience.dataStructures.ContingencyT;
import edu.buffalo.cse.ambience.dataStructures.gyan;
import edu.buffalo.cse.ambience.database.ElementNotFoundException;
import edu.buffalo.cse.ambience.database.TableNotFoundException;

public interface ambienceDBops 
{
	public double getKWII(String vars,String delim) throws IOException,NumberFormatException,ElementNotFoundException;
	public double getPAI(String vars,String delim) throws IOException,NumberFormatException,ElementNotFoundException;
	public double getEntropy(String vars,String delim) throws IOException,NumberFormatException,ElementNotFoundException;
	public ArrayList<gyan> topT(int T,Order order) throws IOException;
	public ContingencyT getCTable(String vars,String delim) throws IOException,NumberFormatException,ElementNotFoundException;
	public ArrayList<gyan> getKWII(ArrayList<String> listg,String delim) throws IOException,NumberFormatException;
}
