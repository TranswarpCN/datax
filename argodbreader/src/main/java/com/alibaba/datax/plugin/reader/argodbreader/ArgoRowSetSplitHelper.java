package com.alibaba.datax.plugin.reader.argodbreader;

import io.transwarp.holodesk.connector.attachment.task.ScanTaskAttachment;
import io.transwarp.holodesk.connector.utils.RowSetUtils;
import io.transwarp.holodesk.connector.utils.RowSetsGroup;
import io.transwarp.shiva.exception.ShivaException;
import io.transwarp.shiva.holo.Distribution;
import io.transwarp.shiva.holo.HoloEngineOperations;
import io.transwarp.shiva.holo.RowSet;
import io.transwarp.shiva.holo.SplitContext;

import java.util.*;

public class ArgoRowSetSplitHelper {

  private static final long TASK_DEFAULT_MEMORY_USAGE = 2 * 1024 * 1024 * 1024;

  // split rowSets to single normal base file + multiple blob files
  public static RowSetsGroup[] splitRowSetsToFiles(Distribution distribution,
                                                   Iterator<RowSet> filterIterator) throws ShivaException {
    // 确保将不同的section的rowset可以区分开
    HashMap<String, TreeMap<byte[], ArrayList<RowSet>>> section2Tablet2RowSets = splitRowSetsToTabletIdToSection(filterIterator);

    ArrayList<RowSetsGroup> rowSetsGroups = new ArrayList<>();
    for (Map.Entry<String, TreeMap<byte[], ArrayList<RowSet>>> kv : section2Tablet2RowSets.entrySet()) {
      String section = kv.getKey();
      TreeMap<byte[], ArrayList<RowSet>> tablet2RowSetsMap = kv.getValue();
      for (Map.Entry<byte[], ArrayList<RowSet>> tablet2RowSets : tablet2RowSetsMap.entrySet()) {
        byte[] tabletId = tablet2RowSets.getKey();
        ArrayList<RowSet> rowSets = tablet2RowSets.getValue();
        // todo: split normal rowsets and blob rowsets
        for (RowSet rowSet : rowSets) {
          String[] hosts = RowSetUtils.getHost(rowSet);
          int bucketId = rowSet.getBaseMeta().getHashValue();
          int rowCount = getRowCount(rowSet);
          long[] rowSetIds = new long[1];
          rowSetIds[0] = rowSet.getId();
          // todo: add blobRowSets to rowSetIds
          SplitContext splitContext = distribution.split(section, tabletId, rowSetIds);
          ScanTaskAttachment attachment = new ScanTaskAttachment(null, bucketId, section);
          rowSetsGroups.add(new RowSetsGroup(new SplitContext[]{splitContext}, hosts, attachment, rowSetIds.length, rowCount, 0, false, TASK_DEFAULT_MEMORY_USAGE));
        }
      }
    }

    RowSetsGroup[] rowSetsGroupsArray = new RowSetsGroup[rowSetsGroups.size()];
    for (int i = 0; i < rowSetsGroupsArray.length; i++) {
      rowSetsGroupsArray[i] = rowSetsGroups.get(i);
    }
    return rowSetsGroupsArray;
  }

  private static HashMap<String, TreeMap<byte[], ArrayList<RowSet>>> splitRowSetsToTabletIdToSection(Iterator<RowSet> rowSetsIterator) {
    // section => tabletId => rowSetIds
    // [Section, [TabletId, Array[RowSet]]]
    HashMap<String, TreeMap<byte[], ArrayList<RowSet>>> rowSetsMap = new HashMap<String, TreeMap<byte[], ArrayList<RowSet>>>();
    while (rowSetsIterator.hasNext()) {
      RowSet rowSet = rowSetsIterator.next();
      String section = rowSet.getSectionName();
      byte[] tabletId = rowSet.getTabletId();
      if (rowSetsMap.containsKey(section)) {
        if (rowSetsMap.get(section).containsKey(tabletId)) {
          rowSetsMap.get(section).get(tabletId).add(rowSet);
        } else {
          ArrayList<RowSet> rowSetBuffer = new ArrayList<>();
          rowSetBuffer.add(rowSet);
          rowSetsMap.get(section).put(tabletId, rowSetBuffer);
        }
      } else {
        ArrayList<RowSet> rowSetBuffer = new ArrayList<>();
        rowSetBuffer.add(rowSet);

        TreeMap<byte[], ArrayList<RowSet>> tabletIdToRowSetBuffer = new TreeMap<>(HoloEngineOperations.getHashPartitionComparator());
        tabletIdToRowSetBuffer.put(tabletId, rowSetBuffer);
        rowSetsMap.put(section, tabletIdToRowSetBuffer);
      }
    }
    return rowSetsMap;
  }

  private static int getRowCount(RowSet rowSet) {
    if (!rowSet.getDeltaFileIterator().hasNext() && rowSet.getBaseMeta().hasRowCount()) {
      return (int) rowSet.getBaseMeta().getRowCount();
    } else {
      return 0;
    }
  }
}
