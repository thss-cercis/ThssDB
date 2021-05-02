/*
 * Session 管理.
 *
 * Author: AyajiLin
 * Date: 2021-05-02
 */
package cn.edu.thssdb.server.sess;

import lombok.SneakyThrows;
import lombok.Synchronized;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SessionManager {
    private final Map<Long, Session> sessionMap = new HashMap<>();

    public SessionManager() {}

    @Synchronized
    @Nonnull
    public Session createSession(String username) {
        Random r = new Random();
        while (true) {
            Long l = r.nextLong();
            if (!sessionMap.containsKey(l)) {
                Session session = new Session(l, username);
                this.sessionMap.put(l, session);
                return session;
            }
        }
    }

    @Synchronized
    @Nullable
    public Session getSession(long sessionID) {
        return this.sessionMap.get(sessionID);
    }

    @SneakyThrows
    @Synchronized
    @Nullable
    public Session destroySession(long sessionID) {
        Session session = this.sessionMap.remove(sessionID);
        if (session != null) {
            session.close();
        }
        return session;
    }
}
