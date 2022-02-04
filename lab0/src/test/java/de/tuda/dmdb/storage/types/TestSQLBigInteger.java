package de.tuda.dmdb.storage.types;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.storage.types.exercise.SQLBigInteger;
import org.junit.Assert;
import org.junit.Test;

public class TestSQLBigInteger extends TestCase {
  @Test
  public void testSerializeDeserialize1() {
    long value = 123456789;

    SQLBigInteger sqlInt = new SQLBigInteger(value);
    byte[] content = sqlInt.serialize();

    SQLBigInteger sqlInt2 = new SQLBigInteger();
    sqlInt2.deserialize(content);

    Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
  }

  @Test
  public void testCompareTo() {
    Long zeroInt = 0L;
    Long oneInt = 1L;
    SQLBigInteger zero = new SQLBigInteger(0L);
    SQLBigInteger one = new SQLBigInteger(1L);

    Assert.assertEquals(zeroInt.compareTo(oneInt), zero.compareTo(one));
    Assert.assertEquals(zeroInt.compareTo(zeroInt), zero.compareTo(zero));
  }

  @Test
  public void testHashCode() {
    Long zeroInt = 0L;
    SQLBigInteger zero = new SQLBigInteger(0);
    SQLBigInteger zero2 = new SQLBigInteger(0);
    SQLBigInteger one = new SQLBigInteger(1);

    Assert.assertEquals(zero.hashCode(), zero2.hashCode());
    Assert.assertTrue(zero.equals(zero2) && zero.hashCode() == zero2.hashCode());
    Assert.assertTrue(one.hashCode() != zero.hashCode());
    Assert.assertTrue(!zero.equals(one) && zero.hashCode() != one.hashCode());
    Assert.assertEquals(zeroInt.hashCode(), zero.hashCode());
  }
}
