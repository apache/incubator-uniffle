<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Contributing to Firestorm
Welcome to [report Issues](https://github.com/Tencent/Firestorm/issues) or [pull requests](https://github.com/Tencent/Firestorm/pulls). It's recommended to read the following Contributing Guide first before contributing. 

## Issues
We use Github Issues to track public bugs and feature requests.

### Search Known Issues First
Please search on [report Issues](https://github.com/Tencent/Firestorm/issues) to avoid creating duplicate issue.

### Reporting New Issues
* Be sure to include a title and clear description, as much relevant information as possible.
* A code sample or an executable test case demonstrating is high recommended.

## Pull Requests
* We use master branch as our developing branch.
* Do not commit/push directly to the master branch. Instead, create a fork and file a pull request.
* When maintaining a branch, merge frequently with the master.
* When maintaining a branch, submit pull requests to the master frequently.
* If you are working on a bigger issue try to split it up into several smaller issues.
* We reserve full and final discretion over whether or not we will merge a pull request. Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

### Make Pull Requests
The code team will monitor all pull request, we run some code check and test on it. After all tests passed, we will accecpt this PR. But it won't merge to `master` branch at once, which have some delay.

Before submitting a pull request, please make sure the followings are done:

1. Fork the repo and create your branch from `master`.
2. Update code or documentation if you have changed APIs.
3. Add the copyright notice to the top of any new files you've added.
4. Check your code lints and checkstyles.
5. Test and test again your code.

## Code Style Guide
Use [Code Style](https://github.com/Tencent/Firestorm/blob/master/checkstyle.xml) for Java.

* 2 spaces for indentation rather than tabs
