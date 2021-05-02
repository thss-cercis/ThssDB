/*
 * Session 的定义.
 *
 * Author: AyajiLin
 * Date: 2021-05-02
 */
package cn.edu.thssdb.server.sess;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Closeable;

@AllArgsConstructor
@Data
public class Session implements Closeable {
    private long sessionID;
    private String username;

    @Override
    public void close() {

    }
}
