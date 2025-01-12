package com.dulno.table.structure;

import com.dulno.core.database.DatabaseDataType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum TableColumnType {
  TEXT (DatabaseDataType.TEXT),
  TEXT_AREA (DatabaseDataType.TEXT),
  DATE (DatabaseDataType.TEXT),
  SWITCH (DatabaseDataType.BOOLEAN),
  CHECKBOX (DatabaseDataType.BOOLEAN);

  private final DatabaseDataType dataType;
}
