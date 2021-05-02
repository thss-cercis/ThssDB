/*
 * RPC 框架的接口实现
 *
 * Author: AyajiLin
 * Date: 2021-05-02
 */
package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetReq;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.server.sess.SessionManager;
import cn.edu.thssdb.server.sess.Session;
import cn.edu.thssdb.utils.enums.StatusCode;
import lombok.AllArgsConstructor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.Date;

@AllArgsConstructor
public class IServiceHandler implements IService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(IServiceHandler.class);
  private final ThssDB db;

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(StatusCode.SUCCESS.code).setMsg(StatusCode.SUCCESS.description));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    ConnectResp resp = new ConnectResp();
    // TODO 因为现在的 database 机制几乎无效，所以对用户名与密码使用 hardcode 实现
    if (!req.getUsername().equals("root") || !req.getPassword().equals("114514")) {
      resp.setStatus(
              new Status(StatusCode.CONNECT_PASSWORD_FAILURE.code)
              .setMsg(StatusCode.CONNECT_PASSWORD_FAILURE.description)
      );
      resp.setSessionId(-1);
      return resp;
    }
    SessionManager sessionManager = db.getSessionManager();
    Session sess = sessionManager.createSession(req.getUsername());

    logger.info("Session 建立: " + sess.getSessionID());

    return new ConnectResp(
            new Status(StatusCode.SUCCESS.code).setMsg(StatusCode.SUCCESS.description),
            sess.getSessionID()
    );
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    Session session = db.getSessionManager().destroySession(req.getSessionId());
    if (session == null) {
      return new DisconnetResp(
              new Status(StatusCode.CONNECT_NOT_FOUND.code)
              .setMsg(StatusCode.CONNECT_NOT_FOUND.description)
      );
    }

    logger.info("Session 销毁: " + session.getSessionID());

    return new DisconnetResp(new Status(StatusCode.SUCCESS.code).setMsg(StatusCode.SUCCESS.description));
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    Session session = db.getSessionManager().getSession(req.getSessionId());
    if (session == null) {
      return new ExecuteStatementResp(
              new Status(StatusCode.CONNECT_NOT_FOUND.code).setMsg(StatusCode.CONNECT_NOT_FOUND.description),
              false,
              false
      );
    }
    // TODO 先解析，然后执行
    return null;
  }
}
