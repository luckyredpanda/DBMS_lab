package de.tuda.dmdb.storage.types;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import org.junit.Assert;
import org.junit.Test;

public class TestSQLVarchar extends TestCase {
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
  public void testCompareTo() {
    String halloString = "hallo";
    String worldString = "world";
    SQLVarchar hallo = new SQLVarchar("hallo", 10);
    SQLVarchar world = new SQLVarchar("world", 10);

    Assert.assertEquals(halloString.compareTo(worldString), hallo.compareTo(world));
    Assert.assertEquals(halloString.compareTo(halloString), hallo.compareTo(hallo));
  }

  @Test
  public void testHashCode() {
    String worldString = "world";
    SQLVarchar hallo = new SQLVarchar("hallo", 10);
    SQLVarchar hallo2 = new SQLVarchar("hallo", 10);
    SQLVarchar world = new SQLVarchar("world", 10);

    Assert.assertEquals(hallo.hashCode(), hallo2.hashCode());
    Assert.assertTrue(hallo.equals(hallo2) && hallo.hashCode() == hallo2.hashCode());
    Assert.assertTrue(hallo.hashCode() != world.hashCode());
    Assert.assertTrue(!hallo.equals(world) && hallo.hashCode() != world.hashCode());
    Assert.assertEquals(world.hashCode(), worldString.hashCode());
  }
}
