package apache.io.utils.example;

import java.io.IOException;

import org.apache.commons.io.IOUtils;

public class Test {
	
	public static void main(String[] args) {
		 String[] sentences;
		
		try {
			sentences = (String[]) IOUtils.readLines(
			    ClassLoader.getSystemClassLoader().getResourceAsStream("500_sentences_en.txt")).toArray(new String[0]);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
		System.out.println(sentences[0]);
	}

}
