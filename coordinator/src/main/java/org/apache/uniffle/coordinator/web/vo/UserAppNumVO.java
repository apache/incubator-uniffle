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

package org.apache.uniffle.coordinator.web.vo;

import java.util.Objects;

public class UserAppNumVO implements Comparable<UserAppNumVO> {

  private String userName;
  private Integer appNum;

  public UserAppNumVO(String userName, Integer appNum) {
    this.userName = userName;
    this.appNum = appNum;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Integer getAppNum() {
    return appNum;
  }

  public void setAppNum(Integer appNum) {
    this.appNum = appNum;
  }

  @Override
  public int compareTo(UserAppNumVO userAppNumVO) {
    return Integer.compare(appNum, userAppNumVO.getAppNum());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UserAppNumVO)) {
      return false;
    }
    UserAppNumVO that = (UserAppNumVO) o;
    return userName.equals(that.userName) && appNum.equals(that.appNum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userName, appNum);
  }
}
