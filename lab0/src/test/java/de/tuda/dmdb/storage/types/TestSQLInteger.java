package de.tuda.dmdb.storage.types;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import org.junit.Assert;
import org.junit.Test;

public class TestSQLInteger extends TestCase {
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
  public void testCompareTo() {
    Integer zeroInt = 0;
    Integer oneInt = 1;
    SQLInteger zero = new SQLInteger(0);
    SQLInteger one = new SQLInteger(1);

    Assert.assertEquals(zeroInt.compareTo(oneInt), zero.compareTo(one));
    Assert.assertEquals(zeroInt.compareTo(zeroInt), zero.compareTo(zero));
  }

  @Test
  public void testHashCode() {
    Integer zeroInt = 0;
    SQLInteger zero = new SQLInteger(0);
    SQLInteger zero2 = new SQLInteger(0);
    SQLInteger one = new SQLInteger(1);

    Assert.assertEquals(zero.hashCode(), zero2.hashCode());
    Assert.assertTrue(zero.equals(zero2) && zero.hashCode() == zero2.hashCode());
    Assert.assertTrue(one.hashCode() != zero.hashCode());
    Assert.assertTrue(!zero.equals(one) && zero.hashCode() != one.hashCode());
    Assert.assertEquals(zeroInt.hashCode(), zero.hashCode());
  }
}
