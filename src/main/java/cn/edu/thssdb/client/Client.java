package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.enums.StatusCode;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  /**
   * 记录 session id，为空表示尚未建立
   */
  @Nullable
  private static Long sessionID;

  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TSocket(host, port);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      StringBuilder builder = new StringBuilder();
      boolean open = true;
      while (true) {
        if (builder.length() == 0) {
          // 全新开始的语句
          print(Global.CLI_PREFIX);
        } else {
          // 之前尚未输完的语句
          print(">  ");
        }
        String lineMsg = SCANNER.nextLine().trim();
        if (!lineMsg.equals(";")) {
          builder.append(' ');
        }
        builder.append(lineMsg);
        if (!lineMsg.endsWith(";")) {
          // 仍未输完指令，继续等待完成
          continue;
        }
        String allCommand = builder.toString().trim();
        long startTime = System.currentTimeMillis();
        // BEGIN switch
        switch (allCommand) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            break;
          case Global.DISCONNECT: {
            if (sessionID == null) {
              println("尚未登录!");
              break;
            }
            disconnect();
            break;
          }
          default: {
            if (allCommand.startsWith(Global.CONNECT)) {
              // connect 语句解析
              connect(allCommand);
            } else {
              // 正常执行语句
              if (sessionID != null) {
                ExecuteStatementResp resp = client.executeStatement(new ExecuteStatementReq(sessionID, allCommand));
                Status status = resp.getStatus();
                if (status.code != StatusCode.SUCCESS.code) {
                  println("Code: " + status.getCode() + ", msg: " + status.getMsg());
                } else {
                  // TODO 格式化输出语句结果
                  println("TODO: Not implemented YET.");
                }
              } else {
                println("尚未连接!");
              }
            }
            break;
          }
        }
        builder.setLength(0);
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void connect(String cmd) {
    List<String> results =
            Splitter.on(CharMatcher.whitespace())
                    .trimResults(CharMatcher.anyOf(";"))
                    .splitToList(cmd);
    if (results.size() != 3) {
      println("Syntax error: connect [username] [password];");
    } else {
      // connect
      ConnectResp resp = null;
      try {
        resp = client.connect(new ConnectReq(results.get(1), results.get(2)));
        Status status = resp.getStatus();
        if (status.code != StatusCode.SUCCESS.code) {
          println("Code: " + status.getCode() + ", msg: " + status.getMsg());
        } else {
          println(String.format("Successfully connect to %s:%s", results.get(1), results.get(2)));
          println("Session ID: " + resp.getSessionId());
          // 成功登录后，设置 sessionID
          sessionID = resp.getSessionId();
        }
      } catch (TException e) {
        e.printStackTrace();
        logger.error(e.getMessage());
      }
    }
  }

  private static void disconnect() {
    DisconnetResp resp = null;
    try {
      assert sessionID != null;
      resp = client.disconnect(new DisconnetReq(sessionID));
      println("Code: " + resp.getStatus().getCode() + " , msg: " + resp.getStatus().getMsg());
    } catch (TException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
        .argName(HELP_NAME)
        .desc("Display help information(optional)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
        .argName(HOST_NAME)
        .desc("Host (optional, default 127.0.0.1)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
        .argName(PORT_NAME)
        .desc("Port (optional, default 6667)")
        .hasArg(false)
        .required(false)
        .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {
    // TODO
    println("DO IT YOURSELF");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
