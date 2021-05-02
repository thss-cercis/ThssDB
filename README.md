# ThssDB
本科大三数据库大作业

## 环境部署

因为此处需要使用 Maven，所以推荐使用 IDE 工具，例如 Intellij Idea（强烈推荐）。下面的以 Idea 为说明：

打开 Project，等待 Idea 处理完 Maven 依赖后，需要我们运行 Maven 的 `compile` 目标。
如果使用 Idea，则在右侧 Maven 工具栏中 可以选择 `LifeCycle - compile`，之后程序会自己下载 thrift 工具于 `target/tools` 中。
当然因为 github 墙的问题，我们**可能**需要将 thrift 工具手动放到此目录下，工具见助教提供的框架中。

运行上述代码后，可能需要将 `target/generated-sources` 文件添加入 classpath 中，如果 IDE 已经自动添加则可省略下述步骤。
在 Idea 中，右键 `target/generated-sources/thrift` 文件夹，选择 `Mark Directory as - Generated Source Root`，之后 Idea 会自动将其添加入我们 IDE 的 classpath 中。
这部分代码为 RPC 框架自动生成，我们并不需要修改它。

到此为止，配置完成，准备迎接大作业。
