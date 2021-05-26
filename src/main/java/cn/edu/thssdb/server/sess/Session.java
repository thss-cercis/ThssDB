/*
 * Session 的定义.
 *
 * Author: AyajiLin
 * Date: 2021-05-02
 */
package cn.edu.thssdb.server.sess;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Closeable;

@AllArgsConstructor
@Data
public class Session implements Closeable {
    private long sessionID;
    private String username;
    // TODO: 写死数据库
    private static final Database currentDatabase;
    static {
      currentDatabase = Manager.getInstance().getDatabases().get("thss");
    }

    @Override
    public void close() {

    }
}
