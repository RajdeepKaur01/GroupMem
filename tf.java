import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.io.BufferedReader;

public class tf
{
  public static void main(String[] args) throws IOException
  {
    if (args.length == 0){
      System.out.println("Need at least one input file");
      return;
    }

    String prefix = args[0];
    for (int i = 1; i < args.length; i++)
    {
      HashMap<String, Integer> keymap = new HashMap<String, Integer>();
      File file = new File(args[i]);
      BufferedReader br = new BufferedReader(new FileReader(file));

      int currentchar;
      String key = "";
      int wordnum = 0;
      int charnum = 0;
      boolean emptyfile = true;
      char temp [] = new char[2048];

      while ((currentchar = br.read()) != -1) {
        emptyfile = false;
        //System.out.println(currentchar);
        if (((currentchar >= 65) && (currentchar <= 90))||((currentchar >= 97) && (currentchar <= 122))||((currentchar >= 48) && (currentchar <= 57))){
          temp[charnum++] = (char) currentchar;
        }
        else
        {
          // if the first of word is symbol, ignore the empty word
          if (charnum == 0)
            continue;

          wordnum++;
          temp[charnum] = '\0';
          key = key.concat(new String(temp));
          if (keymap.containsKey(key)){
            int value = (Integer) keymap.get(key) + 1;
            keymap.put(key, value);
          }
          else
            keymap.put(key,1);
          key = "";
          charnum = 0;
        }
      }
      br.close();
      // add the last word
      if ((emptyfile == false) && (charnum != 0)){
        key = key.concat(new String(temp));
        if (keymap.containsKey(key)){
          int value = (Integer) keymap.get(key) + 1;
          keymap.put(key, value);
        }
        else
          keymap.put(key,1);
        wordnum++;
      }

      //System.out.println(keymap.values());
      ToFile(prefix,wordnum,keymap,args[i]);
    }

  }

  static void ToFile(String prefix, int wordtotal, HashMap<String, Integer> keyMap,String doc) throws IOException{
    Set<String> keys = keyMap.keySet();
    for(String key: keys){
      String filename = prefix+"_"+key;
//      File outfile = new File(filename);
//
//      if (outfile.createNewFile()){
//        System.out.println(outfile.getName()+" is created!");
//      }else{
//        System.out.println(outfile.getName()+" already exists.");
//      }

      int wordc = (Integer) keyMap.get(key);
      //System.out.println("wordc = "+wordc+",wordtotal = "+wordtotal);
      FileWriter buf_writer = new FileWriter(filename);
      buf_writer.write(Double.toString((double)wordc/(double)wordtotal)+"\t" + doc + "\n");
      buf_writer.close();
    }
  }
}
