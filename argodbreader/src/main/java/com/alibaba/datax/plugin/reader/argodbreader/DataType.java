package com.alibaba.datax.plugin.reader.argodbreader;

public enum  DataType {
  BOOLEAN("BOOLEAN"),
  BYTE("TINYINT"),
  SHORT("SMALLINT"),
  INT("INT"),
  LONG("BIGINT"),
  FLOAT("FLOAT"),
  DOUBLE("DOUBLE"),
  CHAR("CHAR"),
  STRING("STRING"),
  VARCHAR("VARCHAR"),
  VARCHAR2("VARCHAR2"),
  DECIMAL("DECIMAL"),
  TIMESTAMP("TIMESTAMP"),
  DATE("DATE");


  private String dataType = null;

  DataType(String dataType) {
    this.dataType = dataType;
  }

  public String getDataType() {
    return dataType;
  }

  public static DataType genDataType(String typeStr) {
    for (DataType dataType : DataType.values()) {
      if (dataType.dataType.equals(typeStr)) {
        return dataType;
      }
    }
    throw new RuntimeException("Not support dataType: " + typeStr);
  }
}
