package de.tuda.dmdb.storage.exercise;

import de.tuda.dmdb.storage.AbstractDiskManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

import java.io.*;

/**
 * @author melhindi, danfai
 *     <p>The DiskManager takes care of the allocation and deallocation of pages within a database.
 *     It performs the reading and writing of pages to and from disk, providing a logical file layer
 *     within the context of a database management system.
 */
public class DiskManager extends AbstractDiskManager {

  public DiskManager() {
    super();
    try {//constructor of file class having file as argument
      File file = new File(DB_FILENAME);
      FileInputStream fis = new FileInputStream(file);     //opens a connection to an actual file
      System.out.println("file content: ");
      int r = 0;
      while ((r = fis.read()) != -1) {
        System.out.print((char) r);        //prints the content of the file
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    // TODO open file
  }

  @Override
  public void close() {
    return;
    // TODO close file
  }
  @Override
  public AbstractPage readPage(Integer pageId) {
    // TODO implement me
    File f = new File(DiskManager.DB_FILENAME);
    int Line_Number = 0;
    String[] arr =null;
    try {
      FileInputStream in = new FileInputStream(f);  // 按照流
      String str = null;
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
      String str_page = null;

      while ((str = bufferedReader.readLine()) != null) {
        str_page = str;
        System.out.println(str_page);
        String[] arr_1 = str_page.split(" ");
        Line_Number = Integer.parseInt(arr_1[0].split("=")[1]);
        System.out.println(Line_Number);
        if (Line_Number == (int)pageId) arr=arr_1;
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    if (pageId < 0 || arr == null )
      return null;
    else {
      //读取文件内容
      for (int i=0;i<arr.length;i++) {
        String[] arr_split = arr[i].split("=");
        arr[i] = arr_split[1];
        System.out.println("arr[i]="+arr[i]);
      }
      int Records_Num=(arr.length-1)/3;
      AbstractRecord r1 = new Record(2);
      r1.setValue(0, new SQLInteger());
      r1.setValue(1, new SQLVarchar(10));
      AbstractPage p = new RowPage(r1.getFixedLength());
      for (int i=0;i<Records_Num;i++) {
        AbstractSQLValue a = new SQLInteger();
        a.parseValue(arr[i*3+2]);
        AbstractSQLValue b = new SQLVarchar(10);
        b.parseValue(arr[i*3+3]);
        r1.setValue(0, a);
        r1.setValue(1, b);
        p.insert(r1);
      }
      p.setPageNumber(pageId);
      return p;
    }
  }

  @Override
  public void writePage(AbstractPage page) {
    // TODO implement me
    if (page == null) {
      NullPointerException e = new NullPointerException();
      throw e;
    } else {
      AbstractRecord[] r1 = new AbstractRecord[page.getNumRecords()];
      for (int i=0;i<page.getNumRecords();i++) {
        r1[i] = new Record(2);
        r1[i].setValue(0, new SQLInteger());
        r1[i].setValue(1, new SQLVarchar(10));
        page.read(i,r1[i]);
      }
      System.out.print(r1[0].getValues()[0]+" "+r1[0].getValues()[1]);
      System.out.println();
      System.out.println(page.toString());

      File f = new File(DiskManager.DB_FILENAME);
      try{
        FileOutputStream fos = new FileOutputStream(f,true);
        int page_num = page.getPageNumber();
        fos.write(("PageNumber="+page_num+" ").getBytes());
        for (int i=0;i<page.getNumRecords();i++) {
          fos.write(("RecordsNumber="+(i+1)+" ").getBytes());
          fos.write(r1[i].getValue(0).getType().toString().getBytes());
          fos.write("=".getBytes());
          fos.write(r1[i].getValue(0).toString().getBytes());
          fos.write(" ".getBytes());
          fos.write(r1[i].getValue(1).getType().toString().getBytes());
          fos.write("=".getBytes());
          fos.write(r1[i].getValue(1).toString().getBytes());
          fos.write(" ".getBytes());
        }
        fos.write("\n".getBytes());
        fos.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }


}