-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE DATABASE root.locomotif
CREATE TIMESERIES root.locomotif.d1.s1 WITH DATATYPE=DOUBLE, ENCODING=PLAIN, COMPRESSION=UNCOMPRESSED
CREATE TIMESERIES root.locomotif.d1.s2 WITH DATATYPE=DOUBLE, ENCODING=PLAIN, COMPRESSION=UNCOMPRESSED
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(1,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(2,1,2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(3,2,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(4,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(5,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(6,-1,-2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(7,-2,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(8,-1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(9,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(10,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(11,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(12,-1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(13,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(14,1,2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(15,2,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(16,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(17,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(18,-1,-2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(19,-2,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(20,-1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(21,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(22,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(23,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(24,-1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(25,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(26,1,2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(27,2,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(28,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(29,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(30,-1,-2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(31,-2,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(32,-1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(33,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(34,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(35,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(36,-1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(37,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(38,1,2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(39,2,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(40,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(41,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(42,-1,-2)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(43,-2,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(44,-1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(45,0,1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(46,1,0)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(47,0,-1)
INSERT INTO root.locomotif.d1(timestamp,s1,s2) VALUES(48,-1,0)
