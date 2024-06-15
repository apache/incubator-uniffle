/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs')
const pathName = require('path')

/**
 * Delete folder
 * @param path Path of the folder to be deleted
 */
function deleteFolder(path) {
  let files = []
  if (fs.existsSync(path)) {
    if (fs.statSync(path).isDirectory()) {
      files = fs.readdirSync(path)
      files.forEach((file) => {
        const curPath = path + '/' + file
        if (fs.statSync(curPath).isDirectory()) {
          deleteFolder(curPath)
        } else {
          fs.unlinkSync(curPath)
        }
      })
      fs.rmdirSync(path)
    } else {
      fs.unlinkSync(path)
    }
  }
}

/**
 * Delete file
 * @param path Path of the file to be deleted
 */
function deleteFile(path) {
  if (fs.existsSync(path)) {
    if (fs.statSync(path).isDirectory()) {
      deleteFolder(path)
    } else {
      fs.unlinkSync(path)
    }
  }
}

/**
 * Copy the folder to the specified directory
 * @param from Source directory
 * @param to Target directory
 */
function copyFolder(from, to) {
  let files = []
  // Whether the file exists If it does not exist, it is created
  if (fs.existsSync(to)) {
    files = fs.readdirSync(from)
    files.forEach((file) => {
      const targetPath = from + '/' + file
      const toPath = to + '/' + file

      // Copy folder
      if (fs.statSync(targetPath).isDirectory()) {
        copyFolder(targetPath, toPath)
      } else {
        // Copy file
        fs.copyFileSync(targetPath, toPath)
      }
    })
  } else {
    mkdirsSync(to)
    copyFolder(from, to)
  }
}

// Create a directory synchronization method recursively
function mkdirsSync(dirname) {
  if (fs.existsSync(dirname)) {
    return true
  } else {
    if (mkdirsSync(pathName.dirname(dirname))) {
      fs.mkdirSync(dirname)
      return true
    }
  }
}

module.exports = {
  deleteFolder,
  deleteFile,
  copyFolder
}
