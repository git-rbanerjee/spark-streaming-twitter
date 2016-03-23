package com.rabin.spark.twitter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class WordReader {
	
	
	public static Set<String> readWords(final String filename)
	{
		Set<String> result = new HashSet<String>();
		try(BufferedReader br = new BufferedReader(new FileReader(filename)))
		{
			
			String word = "";
			
			while((word = br.readLine())!= null )
			{
				result.add(word);
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
}
