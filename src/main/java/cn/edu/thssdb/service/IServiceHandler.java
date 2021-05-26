/*
 * RPC 框架的接口实现
 *
 * Author: AyajiLin
 * Date: 2021-05-02
 */
package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitor;
import cn.edu.thssdb.parser.SQLVisitorImpl;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.QueryResult;
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
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.server.sess.SessionManager;
import cn.edu.thssdb.server.sess.Session;
import cn.edu.thssdb.utils.Cells;
import cn.edu.thssdb.utils.enums.StatusCode;
import lombok.AllArgsConstructor;
import lombok.var;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

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
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(req.getStatement()));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();

    // 写死数据库
    Database database = Manager.getInstance().getDatabases().get("thss");

    try {
      // 解析 query 请求
      List<IQueryRequest> reqList = (List<IQueryRequest>) visitor.visit(parser.parse());

      QueryResult result = null;
      for (IQueryRequest r: reqList) {
        result = r.execute(database);
      }
      assert result != null;
      // QueryResult -> ExecuteStatementResp
      var resp = new ExecuteStatementResp(
        new Status(StatusCode.SUCCESS.code),
        false,
        true
      );
      resp.setColumnsList(new ArrayList<>());
      resp.setRowList(new ArrayList<>());
      for (int i = 0;i < result.getMetaInfo().getColumns().size();++i) {
        // 结果的列名
        String colName = result.getMetaInfo().getColumns().get(i).getName();
        if (result.getMetaInfo().getTableNames() != null && result.getMetaInfo().getTableNames().get(i) != null) {
          resp.getColumnsList().add(String.format("%s.%s", result.getMetaInfo().getTableNames().get(i), colName));
        } else {
          resp.getColumnsList().add(colName);
        }
      }
      for (int i = 0;i < result.getAttrs().size();++i) {
        // 结果
        Cells cells = result.getAttrs().get(i);
        resp.getRowList().add(
          cells.getCells().stream()
            .map(c -> c.getValue() != null? c.getValue().toString() : null)
            .collect(Collectors.toList())
        );
      }
      return resp;
    } catch (Exception e) {
      Status status = new Status(StatusCode.FAILURE.code);
      status.setMsg(e.getMessage());
      return new ExecuteStatementResp(status, false, true);
    }
  }
}
