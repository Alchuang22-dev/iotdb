@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@echo off

pushd %~dp0..\..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

IF EXIST "%IOTDB_HOME%\sbin\windows\stop-confignode.bat" (
  SET CONFIGNODE_STOP_PATH="%IOTDB_HOME%\sbin\windows\stop-confignode.bat"
) ELSE (
  echo "Can't find stop-confignode.bat."
  exit 0
)

IF EXIST "%IOTDB_HOME%\sbin\windows\stop-datanode.bat" (
  SET DATANODE_STOP_PATH="%IOTDB_HOME%\sbin\windows\stop-datanode.bat"
) ELSE (
  echo "Can't find stop-datanode.bat."
  exit 0
)

start cmd /c %CONFIGNODE_STOP_PATH%
TIMEOUT /T 3 /NOBREAK
start cmd /c %DATANODE_STOP_PATH%
exit 0
