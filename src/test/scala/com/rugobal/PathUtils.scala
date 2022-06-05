package com.rugobal

import java.nio.file.Paths

object PathUtils {
  /**
   * Current path in linux style.
   *
   * If in a windows system it will remove the c: and substitute '\' by '/'
   *
   * @return
   */
  def currentPath(): String = {
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      Paths.get("").toAbsolutePath.toString.replace("C:", "").replace("\\", "/")
    } else {
      Paths.get("").toAbsolutePath.toString
    }
  }
}