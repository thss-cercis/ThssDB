// 建表
CREATE TABLE ta(a1 INT NOT NULL, a2 LONG NOT NULL, a3 FLOAT, a4 DOUBLE, a5 STRING(15) NOT NULL, PRIMARY KEY (a1));
CREATE TABLE tb(b1 INT NOT NULL, b2 LONG NOT NULL, b3 FLOAT, b4 DOUBLE, b5 STRING(15) NOT NULL, PRIMARY KEY (b1));
// 查看表的信息
SHOW TABLE ta;
SHOW TABLE tb;
// 插入数据
INSERT INTO ta VALUES(11, 1, 31.0, 41.0, 'hello1');
INSERT INTO ta VALUES(12, 2, 32.0, 42.0, 'hello2');
INSERT INTO ta VALUES(13, 3, 33.0, 43.0, 'hello3');
INSERT INTO ta(a1, a2, a3, a5) VALUES(14, 4, 34.0, 'hello4');
INSERT INTO tb VALUES(51, 1, 71.0, 81.0, 'world1');
INSERT INTO tb VALUES(52, 2, 72.0, 82.0, 'world2');
INSERT INTO tb VALUES(53, 3, 73.0, 83.0, 'world3');
INSERT INTO tb(b1, b2, b3, b5) VALUES(54, 4, 74.0, 'strange4');
// 查询数据
SELECT a1, a3, a4 FROM ta;
SELECT b1, b2 FROM tb;
SELECT ta.a1, tb.b1, ta.a5 FROM ta JOIN tb ON ta.a2 = tb.b2 WHERE ta.a2 = 4;
// 更新数据
UPDATE ta SET a3 = 999 WHERE a1 = 11;
SELECT a1, a3 FROM ta WHERE a1 = 11;
// 删除数据
DELETE FROM ta WHERE a1 = 11;
SELECT a1, a3 FROM ta WHERE a1 = 11;
// 事务
BEGIN;
UPDATE tb SET b5 = 'transaction' WHERE b1 = 51;
COMMIT;
// 事务同时查询
SELECT b1, b5 FROM tb WHERE b1 = 51;
// 清空表
DROP TABLE ta;
DROP TABLE tb;
