

package de.tuda.dmdb.storage.types.advanced;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.storage.types.exercise.SQLBigInteger;
import org.junit.Assert;
import org.junit.Test;

public class TestAdvancedSQLBigInteger extends TestCase {
  @Test
  public void testSerializeDeserialize1() {
    long value = 123456789L;
    SQLBigInteger sqlInt = new SQLBigInteger(value);
    byte[] content = sqlInt.serialize();
    SQLBigInteger sqlInt2 = new SQLBigInteger();
    sqlInt2.deserialize(content);
    Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
  }
  
  @Test
  public void testSerializeDeserialize2() {
    int i;
    for (i = 0; i <= 10000; i++) {
      long value = (long)(Math.random() * 9.223372036854776E18D);
      SQLBigInteger sqlInt = new SQLBigInteger(value);
      byte[] content = sqlInt.serialize();
      SQLBigInteger sqlInt2 = new SQLBigInteger();
      sqlInt2.deserialize(content);
      Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
    } 
    for (i = 0; i <= 10000; i++) {
      long value = (long)(Math.random() * -9.223372036854776E18D);
      SQLBigInteger sqlInt = new SQLBigInteger(value);
      byte[] content = sqlInt.serialize();
      SQLBigInteger sqlInt2 = new SQLBigInteger();
      sqlInt2.deserialize(content);
      Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
    } 
  }
}

