package com.alibaba.datax.plugin.reader.argodbreader;

import io.transwarp.holodesk.connector.scanner.RemoteFetchClient;
import io.transwarp.holodesk.connector.scanner.UpdateRowAttachmentIterator;
import io.transwarp.holodesk.connector.utils.RemoteFetchRowSetsGroup;
import io.transwarp.holodesk.connector.utils.RowSetSplitHelper;
import io.transwarp.holodesk.connector.utils.RowSetUtils;
import io.transwarp.holodesk.core.UnitScanner;
import io.transwarp.holodesk.core.common.Block;
import io.transwarp.holodesk.core.common.RowSetElement;
import io.transwarp.holodesk.core.common.ScannerIteratorType;
import io.transwarp.holodesk.core.iterator.RowResultIterator;
import io.transwarp.holodesk.core.options.ReadOptions;
import io.transwarp.holodesk.core.rowkey.InternalRowKey;
import io.transwarp.holodesk.core.scanner.ScannerFactory;
import io.transwarp.holodesk.core.schema.RowSetMetaData;
import io.transwarp.nucleon.scheduler.TaskLevelExecutorFailureException;
import io.transwarp.nucleon.storage.HolodeskBlockManager;
import io.transwarp.shiva.client.ShivaClient;
import io.transwarp.shiva.engine.holo.FileMetaPB;
import io.transwarp.shiva.grpc.Utils;
import io.transwarp.shiva.holo.HoloClient;
import io.transwarp.shiva.holo.RowSet;
import io.transwarp.shiva.holo.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

public class ScanClient {
  private static final Logger LOG = LoggerFactory.getLogger(ScanClient.class);
  public static RowResultIterator scan(ShivaClient shivaClient,
                                       HoloClient holoClient,
                                       String tableName,
                                       ReadOptions readOptions,
                                       RowSet[] rowSets,
                                       InternalRowKey[] rowKeyList) throws Exception {
    ScannerIteratorType scannerIteratorType = readOptions.getScannerIteratorType();
    RowSet[] _rowSets = rowSets;

    if (rowKeyList != null) {
      if (_rowSets.length != 1) {
        throw new RuntimeException("[Holodesk] Only support split to files when rowKeyList is not null, but length of normal rowSets is ${_rowSets.length}");
      }
    }
    boolean containsNonLocalRowSet = false;
    for (RowSet rowSet : _rowSets) {
      if (isLocalRowSet(rowSet)) {
        containsNonLocalRowSet = true;
        break;
      }
    }

    if (containsNonLocalRowSet) {
      Table table = holoClient.openTable(tableName);
//      if (!readOptions.isCanDoRowBlockScan()) {
      if (true) {
        RemoteFetchRowSetsGroup[] rowSetsGroup = RowSetSplitHelper.splitRowSetsToTablets(Arrays.asList(_rowSets).iterator(), new RowSet[0]);
        return new UpdateRowAttachmentIterator(_rowSets[0], RemoteFetchClient.fetch(shivaClient,
          table, rowSetsGroup, readOptions));
      } else {
        // row block bulk scan
        LOG.info("Execute row block bulk scan.");
        RowSetElement[] rowSetElements = new RowSetElement[_rowSets.length];
        for (int i = 0; i < _rowSets.length; i++) {
          rowSetElements[i] = getSuitableRowSet(table, shivaClient, _rowSets[i]);
        }
        UnitScanner scanner = ScannerFactory.newScanner(readOptions, rowSetElements);
        return scanner.newRowIterator();
      }
    } else {
      // local read
      LOG.info("All rowSets are local rowSet, so execute local read.");
      try {
        RowSetElement[] rowSetElements = getRowSetElements(_rowSets);
        UnitScanner scanner = ScannerFactory.newScanner(readOptions, rowSetElements, null);
        return scanner.newRowIterator();
      } catch (Exception ex){
        TaskLevelExecutorFailureException e = new TaskLevelExecutorFailureException(ex.getMessage());
        e.initCause(ex);
        throw e;
      }

    }
  }

  protected static boolean isLocalRowSet(RowSet rowSet) throws UnknownHostException {
    String[] hosts = RowSetUtils.getHost(rowSet);
//    String hostIp = "";
//    String currentHost =  HolodeskBlockManager.getIpToHost(hostIp, HolodeskBlockManager.getHolodeskBlockManager().hostCache());
    String currentHost = IpUtils.getLocalHostName();
    boolean isLocalRowSet = false;
    for (String tmpHost : hosts) {
      if (tmpHost.equalsIgnoreCase(currentHost)) {
        isLocalRowSet = true;
        break;
      }
    }
    if (!isLocalRowSet) {
      StringBuilder sb = new StringBuilder();
      for (String tmpHost : hosts) {
        sb.append(tmpHost).append(",");
      }
      LOG.info(String.format("read remote rowset %s, tablet: %s, section: %s, current host: %s, rowset host: %s",
        rowSet.getId(), rowSet.getTabletName(), rowSet.getSectionName(), currentHost, sb.toString()));
    }
    return isLocalRowSet;
  }

  protected static RowSetElement getSuitableRowSet(Table table,
                                                   ShivaClient shivaClient,
                                                   RowSet replicateRowSet) throws Exception {
    String currentHost = IpUtils.getLocalHostName();
    RowSet.File baseFile = replicateRowSet.getBaseFile();
    FileMetaPB baseFileMeta = replicateRowSet.getBaseMeta();
    HashMap<String, RowSet.Location> hostnameToLocation = new HashMap<String, RowSet.Location>();
    for (RowSet.Location location : baseFile.getLocations()) {
      String hostIp = Utils.serverIdToIPAddress(location.getServerId()).split(":")[0];
      String hostName = HolodeskBlockManager.getIpToHost(hostIp, HolodeskBlockManager.getHolodeskBlockManager().hostCache());
      hostnameToLocation.put(hostName, location);
    }

    String targetHost = null;
    for (String k : hostnameToLocation.keySet()) {
      if (k.equalsIgnoreCase(currentHost)) {
        targetHost = k;
        break;
      }
    }
    if (targetHost != null) {
      RowSet.Location location = hostnameToLocation.get(targetHost);
      String filePath = location.getPath() + "/" + baseFile.getName();
      Block baseBlock = new Block(filePath, baseFileMeta.getEffectiveLength(), baseFileMeta.getVersion());
      ArrayList<Block> deltaBlocks = new ArrayList<>();
      Iterator<RowSet.File> deltaFileIter = replicateRowSet.getDeltaFileIterator();
      while (deltaFileIter.hasNext()) {
        RowSet.File delta = deltaFileIter.next();
        FileMetaPB deltaMeta = delta.getMeta();
        String deltaFileName = delta.getName();
        List<RowSet.Location> deltaLocations = delta.getLocations();
        RowSet.Location targetDeltaLocation = null;
        for (RowSet.Location deltaLoc : deltaLocations) {
          if (deltaLoc.getServerId() == location.getServerId()) {
            targetDeltaLocation = deltaLoc;
            break;
          }
        }
        String deltaFilePath = targetDeltaLocation.getPath() + "/" + deltaFileName;
        deltaBlocks.add(new Block(deltaFilePath, deltaMeta.getEffectiveLength(), deltaMeta.getVersion()));
      }
      int[] localColumnIds = io.transwarp.holodesk.connector.util.ColumnIdHelper.getRowSetColumnIds(replicateRowSet);
      RowSetMetaData rowSetMetaData = new RowSetMetaData(replicateRowSet.getSectionName(), replicateRowSet.getTabletId(),
        replicateRowSet.getId(), localColumnIds);
      return new RowSetElement(baseBlock, (Block[]) deltaBlocks.toArray(), rowSetMetaData, null);
    } else {
      throw new RuntimeException("Not support RowBlock Remote Read");
//      String[] remoteHosts = (String[]) hostnameToLocation.keySet().toArray();
//      if (remoteHosts.length != 0) {
//        // remote read
//        RowSet.Location location = hostnameToLocation.get(remoteHosts[0]);
//        String filePath = location.getPath() + "/" + baseFile.getName();
//        RemoteBlock baseBlock = new RemoteBlock(filePath, baseFileMeta.getEffectiveLength(), baseFileMeta.getVersion());
//        baseBlock.init(shivaClient, table, replicateRowSet.getSectionName(), replicateRowSet.getTabletId(), location.getServerId(),
//          baseFile.getName(), baseBlock.isCommitted());
//
//        ArrayList<RemoteBlock> deltaBlocks = new ArrayList<>();
//        Iterator<RowSet.File> deltaFileIter = replicateRowSet.getDeltaFileIterator();
//        while (deltaFileIter.hasNext()) {
//          RowSet.File delta = deltaFileIter.next();
//          FileMetaPB deltaMeta = delta.getMeta();
//          String deltaFileName = delta.getName();
//          List<RowSet.Location> deltaLocations = delta.getLocations();
//          RowSet.Location targetDeltaLocation = null;
//          for (RowSet.Location deltaLoc : deltaLocations) {
//            if (deltaLoc.getServerId() == location.getServerId()) {
//              targetDeltaLocation = deltaLoc;
//              break;
//            }
//          }
//          String deltaFilePath = targetDeltaLocation.getPath() + "/" + deltaFileName;
//          RemoteBlock deltaBlock = new RemoteBlock(deltaFilePath, deltaMeta.getEffectiveLength(), deltaMeta.getVersion());
//          deltaBlock.init(shivaClient, table, replicateRowSet.getSectionName(), replicateRowSet.getTabletId(),
//            targetDeltaLocation.getServerId(), delta.getName(), delta.isCommitted());
//          deltaBlocks.add(deltaBlock);
//        }
//        int[] localColumnIds = io.transwarp.holodesk.connector.util.ColumnIdHelper.getRowSetColumnIds(replicateRowSet);
//        RowSetMetaData rowSetMetaData = new RowSetMetaData(replicateRowSet.getSectionName(), replicateRowSet.getTabletId(),
//          replicateRowSet.getId(), localColumnIds);
//        return new RowSetElement(baseBlock, (Block[]) deltaBlocks.toArray(), rowSetMetaData, null);
//      } else {
//        throw new RuntimeException("No hosts found for all the " +
//          "replicates of file " + baseFile.getLocations().get(0).getPath() + "/" + baseFile.getName());
//      }
    }
  }

  //
  private static RowSetElement getSuitableLocalRowSet(RowSet replicateRowSet) throws UnknownHostException {
    String currentHost = IpUtils.getLocalHostName();
    RowSet.File baseFile = replicateRowSet.getBaseFile();
    FileMetaPB baseFileMeta = replicateRowSet.getBaseMeta();
    HashMap<String, RowSet.Location> hostnameToLocation = new HashMap<String, RowSet.Location>();
    for (RowSet.Location location : baseFile.getLocations()) {
      String hostIp = Utils.serverIdToIPAddress(location.getServerId()).split(":")[0];
      String hostName = HolodeskBlockManager.getIpToHost(hostIp, HolodeskBlockManager.getHolodeskBlockManager().hostCache());
      hostnameToLocation.put(hostName, location);
    }

    String targetHost = null;
    for (String k : hostnameToLocation.keySet()) {
      if (k.equalsIgnoreCase(currentHost)) {
        targetHost = k;
        break;
      }
    }
    if (targetHost != null) {
      RowSet.Location location = hostnameToLocation.get(targetHost);
      String filePath = location.getPath() + "/" + baseFile.getName();
      Block baseBlock = new Block(filePath, baseFileMeta.getEffectiveLength(), baseFileMeta.getVersion());
      ArrayList<Block> deltaBlocks = new ArrayList<>();
      Iterator<RowSet.File> deltaFileIter = replicateRowSet.getDeltaFileIterator();
      while (deltaFileIter.hasNext()) {
        RowSet.File delta = deltaFileIter.next();
        FileMetaPB deltaMeta = delta.getMeta();
        String deltaFileName = delta.getName();
        List<RowSet.Location> deltaLocations = delta.getLocations();
        RowSet.Location targetDeltaLocation = null;
        for (RowSet.Location deltaLoc : deltaLocations) {
          if (deltaLoc.getServerId() == location.getServerId()) {
            targetDeltaLocation = deltaLoc;
            break;
          }
        }
        String deltaFilePath = targetDeltaLocation.getPath() + "/" + deltaFileName;
        deltaBlocks.add(new Block(deltaFilePath, deltaMeta.getEffectiveLength(), deltaMeta.getVersion()));
      }
      int[] localColumnIds = io.transwarp.holodesk.connector.util.ColumnIdHelper.getRowSetColumnIds(replicateRowSet);
      RowSetMetaData rowSetMetaData = new RowSetMetaData(replicateRowSet.getSectionName(), replicateRowSet.getTabletId(),
        replicateRowSet.getId(), localColumnIds);
      return new RowSetElement(baseBlock, (Block[]) deltaBlocks.toArray(), rowSetMetaData, null);
    } else {
      throw new RuntimeException("This is not local rowset!");
    }
  }

  protected static RowSetElement[] getRowSetElements(RowSet[] rowSets) throws UnknownHostException {
    RowSetElement[] rowSetElements = new RowSetElement[rowSets.length];
    for (int i = 0; i < rowSetElements.length; i++) {
      rowSetElements[i] = getSuitableLocalRowSet(rowSets[i]);
    }
    return rowSetElements;
  }

}
