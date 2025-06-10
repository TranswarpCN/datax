package com.alibaba.datax.plugin.reader.argodbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ArgoDBReaderErrorCode implements ErrorCode {

  /**
   *
   */
  BAD_CONFIG_VALUE("ArgoDBReader-00", "The value you configured is invalid."),
  FAIL_CLIENT_CONNECT("ArgoDBReader-02", "GDB connection is abnormal."),
  UNSUPPORTED_TYPE("ArgoDBReader-03", "Unsupported data type conversion."),
  FAIL_ROWSET_GROUPS("ArgoDBReader-04", "Error pulling all labels, it is recommended to configure the specified label pull."),
    ;

  private final String code;
  private final String description;

  private ArgoDBReaderErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return String.format("Code:[%s], Description:[%s]. ", this.code,
      this.description);
  }
}
