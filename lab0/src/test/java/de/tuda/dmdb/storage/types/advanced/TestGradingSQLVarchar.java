package de.tuda.dmdb.storage.types.advanced;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import org.junit.Assert;
import org.junit.Test;

public class TestGradingSQLVarchar extends TestCase {
  @Test
  public void testSerializeDeserialize1() {
    String value = "123456789";
    SQLVarchar sqlVarchar = new SQLVarchar(value, 255);
    byte[] data = sqlVarchar.serialize();
    SQLVarchar sqlVarchar2 = new SQLVarchar(255);
    sqlVarchar2.deserialize(data);
    Assert.assertEquals(sqlVarchar.getValue(), sqlVarchar2.getValue());
  }
  
  @Test
  public void testSerializeDeserialize2() {
    for (int i = 0; i <= 10000; i++) {
      int length = (int)(Math.random() * 255.0D);
      StringBuffer valueBuffer = new StringBuffer();
      for (int j = 0; j < length; j++) {
        char c = (char)(int)(Math.random() * 127.0D);
        valueBuffer.append(c);
      } 
      String value = valueBuffer.toString();
      SQLVarchar sqlVarchar = new SQLVarchar(value, 255);
      byte[] data = sqlVarchar.serialize();
      SQLVarchar sqlVarchar2 = new SQLVarchar(255);
      sqlVarchar2.deserialize(data);
      Assert.assertEquals(sqlVarchar.getValue(), sqlVarchar2.getValue());
    } 
  }
}