import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Scanner;

public class tf
{
	public static void main(String[] args) throws IOException
	{
        if (args.length == 0){
            System.out.println("Need at least one input file");
            return;
        }
        
        String prefix = args[0];  
        // remove last time output file
        // removePrev(prefix);
        for (int i = 1; i < args.length; i++)
        {
        	LinkedList<String> keylist = new LinkedList<String>();
            File file = new File(args[i]);
            BufferedReader br = new BufferedReader(new FileReader(file));
            
            int currentchar;
            String key = "";
            int wordnum = 0;
            int charnum = 0;
            boolean emptyfile = true;
            while ((currentchar = br.read()) != -1) {
            	emptyfile = false;
                //System.out.println(currentchar);
                if (((currentchar >= 65) && (currentchar <= 90))||((currentchar >= 97) && (currentchar <= 122))||((currentchar >= 48) && (currentchar <= 57))){
                    
                    charnum++;
                	char temp [] = new char[1];
                	temp[0] = (char) currentchar;
                    key = key.concat(new String(temp));
                }
                else
                {
                    // if the first of word is symbol, ignore the empty word
                    if (charnum == 0)
                        continue;
                    
                	keylist.add(key);
                	wordnum++;
                	String filename = prefix+"_"+key;
                	
                	RandomAccessFile raf = new RandomAccessFile(filename, "rws");
                    raf.seek(raf.length());
                    raf.writeBytes(key);
                    raf.writeBytes("\n");
                    raf.close();
                    
                	key = "";
                    charnum = 0;
                }
            }
            // add the last word
            if ((emptyfile == false) && (charnum != 0)){
            	keylist.add(key);
            	wordnum++;
            	String filename = prefix+"_"+key;
            	
            	RandomAccessFile raf = new RandomAccessFile(filename, "rws");
                raf.seek(raf.length());
                raf.writeBytes(key);
                raf.writeBytes("\n");
                raf.close();
            }
            // check duplicate
            RemoveDuplicate(keylist);
            //for (int j = 0; j < keylist.size(); j++)
            //    System.out.println(keylist.get(j));
            SumFile(prefix,wordnum,keylist,args[i]);
        }
        
    }
	static void RemoveDuplicate(LinkedList<String> keylist){
		
		for (int i = 0; i < keylist.size(); i++)
			for (int j = i+1; j < keylist.size(); j++)
                if ((keylist.get(i)).equals(keylist.get(j))){
                    keylist.remove(j);
                    j--;    
                    // because if we delete this one, the next one become j 
                    // and if we test j++, then we miss the current one
                }
	}
    
	static void SumFile(String prefix, int wordtotal, LinkedList<String> keylist,String doc) throws IOException{
		
		for (int i = 0; i < keylist.size(); i++){
			String filename = prefix+"_"+keylist.get(i);
			//System.out.println(filename);
			File infile = new File(filename);
			
			Scanner sc2 = null;
			try {
				sc2 = new Scanner(infile);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
			int wordc = 0; 
			while (sc2.hasNextLine()) {
                sc2.nextLine();
				wordc++;
			}
			
			// delete the file
			if(infile.delete()){
	            System.out.println(infile.getName() + " is deleted!");
	        }else{
	            System.out.println("Delete "+infile.getName()+" is failed.");
	        }
			
            if (infile.createNewFile()){
                System.out.println(infile.getName()+" is created!");
            }else{
                System.out.println(infile.getName()+" already exists.");
            }
            
            System.out.println("wordc = "+wordc+",wordtotal = "+wordtotal);
            RandomAccessFile raf = new RandomAccessFile(filename, "rws");
            raf.writeBytes(Double.toString((double)wordc/(double)wordtotal)+"\t");
            raf.writeBytes(doc + "\n");
            raf.close();
		}
	}
    /*
    public static void removePrev(final String prefix)
	{
		String pathname = System.getProperty("user.dir");
        
        File folder = new File(pathname);
        File [] files = folder.listFiles(new FilenameFilter() {
            @Override
         	public boolean accept( File dir, String name ) {
                return name.startsWith(prefix);
            }
        } );
        
        for ( File file : files )
        	if(file.delete())
                System.out.println(file.getName() + " is deleted!");
            else
                System.out.println("Delete "+file.getName()+" is failed.");
        
	}*/
}
