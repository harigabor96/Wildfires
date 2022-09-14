package org.wildfires.service

import java.nio.file.Files
import java.nio.file.Paths

object FileService {

  def createDirectoryIfNotExist(directoryPath: String): Unit = {
    Files.createDirectories(Paths.get(directoryPath))
  }

  def removePathPrefix(path: String, prefix: String): String = {
    path.substring(prefix.length, path.length)
  }

}
