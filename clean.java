import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Scanner;

public class clean
{
	public static void main(final String[] args) throws IOException
	{
		String pathname = System.getProperty("user.dir");
        
        File folder = new File(pathname);
        File [] files = folder.listFiles(new FilenameFilter() {
            @Override
         	public boolean accept( File dir, String name ) {
                return name.startsWith(args[0]);
            }
        } );
        
        for ( File file : files )
        	if(file.delete())
                System.out.println(file.getName() + " is deleted!");
            else
                System.out.println("Delete "+file.getName()+" is failed.");
	}
}
