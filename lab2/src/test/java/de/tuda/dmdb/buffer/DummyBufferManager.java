package de.tuda.dmdb.buffer;

import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.EnumPageType;
import de.tuda.dmdb.storage.exercise.RowPage;
import java.util.HashMap;

public class DummyBufferManager extends BufferManagerBase {

  private HashMap<Integer, AbstractPage> map = new HashMap<>();

  @Override
  public AbstractPage pin(Integer pageId) {
    this.pageTable.put(pageId, map.get(pageId));
    return map.get(pageId);
  }

  @Override
  public void unpin(Integer pageId) {
    this.pageTable.remove(pageId);
  }

  @Override
  public AbstractPage createPage(EnumPageType type, byte[] data) {
    AbstractPage page = null;
    switch (type) {
      case RowPageType:
        page = new RowPage(data);
        break;
      default:
        throw new IllegalArgumentException("You passed an unsupported page type");
    }
    if (!map.containsKey(page.getPageNumber())) map.put(page.getPageNumber(), page);
    return page;
  }

  @Override
  public AbstractPage createDefaultPage(EnumPageType type, int slotSize) {
    AbstractPage page = null;
    switch (type) {
      case RowPageType:
        page = new RowPage(slotSize);
        break;
      default:
        throw new IllegalArgumentException("You passed an unsupported page type");
    }
    page.setPageNumber(map.size());
    map.put(page.getPageNumber(), page);
    return page;
  }
}
