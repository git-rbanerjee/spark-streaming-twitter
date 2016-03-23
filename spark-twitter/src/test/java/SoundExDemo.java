

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.language.Soundex;

public class SoundExDemo {

	public static void main(String[] args) throws EncoderException {
		// TODO Auto-generated method stub
			Soundex sx = new Soundex();
			String[] xxx = {"Hi","Hiii","Hiiii", "Him","LuvTwitter","Helllo","LoveTwitter","Hello","GoodNight","GooodNight","News","Nike"};
			Map<String, String> rer = new HashMap<>();
			int[] aaa = new int[xxx.length];
			
			for(int i=0;i<aaa.length;i++)
				aaa[i] = -1;
			
 			for(int i =0 ;i<xxx.length ;i++)
			{
 				System.out.println(sx.encode(xxx[i]));
				for(int j=i+1;j<xxx.length;j++){
					boolean similar = sx.difference(xxx[i], xxx[j])==4;
					if(aaa[j] == -1 && similar){
						aaa[j] = i;
						System.out.println(xxx[i] + "-->" +xxx[j]+"-"+i +"::"+j);
					}
				}
			}
 			for(int i=0;i<xxx.length;i++){
 				if(aaa[i] == -1)
 					System.out.print(xxx[i]+",");
 			}
			for(int i =0 ;i<xxx.length ;i++){
				String key = sx.encode(xxx[i]);
				if(!rer.containsKey(key)){
					rer.put( key, xxx[i]);
				}
				else{
					String cont = rer.get(key)+"/"+xxx[i];
					rer.put(key, cont);
				}
			}
			System.out.println(rer);
			//System.out.println(sx.difference("LoveTwitter", "LuvTwitter")==4?"Similar":"Not-Similar");
			
	}

}
