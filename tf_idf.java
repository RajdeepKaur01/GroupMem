import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;

public class tf_idf {

	public static long countLines(String filename) throws IOException {
		LineNumberReader reader  = new LineNumberReader(new FileReader(filename));
		long cnt = 0;
		String lineRead = "";
		while ((lineRead = reader.readLine()) != null) {}
		cnt = reader.getLineNumber(); 
		reader.close();
		return cnt;
	}

	public static void main(String[] args) throws IOException
	{
		long DOC_COUNT = 5;
		if (args.length < 1){
			System.out.println("Requires file name");
			return;
		}

		String fileName = args[0];
		String[] splitFileName = fileName.split("_");
		String key = splitFileName[1];
		long totalDocs = DOC_COUNT;
		long totalReferredDocs = countLines(fileName);
		double idf = Math.log((double)(totalDocs + 1)/(double)totalReferredDocs);

		File infile = new File(fileName);
		RandomAccessFile outFile = new RandomAccessFile("tfidf_" + fileName ,"rws");
		BufferedReader br = new BufferedReader(new FileReader(infile));
		String line;
		while((line = br.readLine()) != null) {
			String[] splitline = line.split("\t");
			String Doc = splitline[1];
			double tf = Double.valueOf(splitline[0]);
			double tf_idf = tf * (1/idf);
			outFile.writeBytes(key +"\t");
			outFile.writeBytes(Double.toString(tf_idf)+"\t");
			outFile.writeBytes(Doc + "\n");		
		}
		br.close();
		outFile.close();
	}
}
