package edu.buffalo.cse.ambience.dataStructures;

public class Combination
{
	String comb_str; //the combination as a string e.g. 1|2|3|.
	double kwii;//KWII
    double pai;//PAI
    double pvalue = -100;             
    
	public Combination(String str)
	{
            comb_str = str;
            kwii = -100;
            pai = -100;
	}

	public Combination(Combination C)
	{
            comb_str = new String(C.comb_str);
            kwii = -100;
            pai = -100;
            pvalue = C.pvalue;
	}

	public Combination()
	{
            comb_str = null;
            kwii = -100;
            pai = -100;
	}
}
