

package de.tuda.dmdb.storage.types.advanced;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import org.junit.Assert;
import org.junit.Test;

public class TestGradingSQLInteger extends TestCase {
  @Test
  public void testSerializeDeserialize1() {
    int value = 123456789;
    SQLInteger sqlInt = new SQLInteger(value);
    byte[] content = sqlInt.serialize();
    SQLInteger sqlInt2 = new SQLInteger();
    sqlInt2.deserialize(content);
    Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
  }
  
  @Test
  public void testSerializeDeserialize2() {
    int i;
    for (i = 0; i <= 10000; i++) {
      int value = (int)(Math.random() * 2.147483647E9D);
      SQLInteger sqlInt = new SQLInteger(value);
      byte[] content = sqlInt.serialize();
      SQLInteger sqlInt2 = new SQLInteger();
      sqlInt2.deserialize(content);
      Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
    } 
    for (i = 0; i <= 10000; i++) {
      int value = (int)(Math.random() * -2.147483648E9D);
      SQLInteger sqlInt = new SQLInteger(value);
      byte[] content = sqlInt.serialize();
      SQLInteger sqlInt2 = new SQLInteger();
      sqlInt2.deserialize(content);
      Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
    } 
  }
}
