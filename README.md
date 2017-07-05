# go-mysql-diff
Compare two mysql database triggers, functions, tables, columns and indexing tools

# 配置文件利用toml
格式如下：

```
# schema with the same name servers 
schemaname1 = "ch1"

schemaname2 = "ch2"

[servers]
  # You can indent as you please. Tabs or spaces. TOML don't care.
  [servers.1]
  host = "127.0.0.1"
  port = "3306"
  user = "ch"
  password = "123456"
  name = "ch1"

  [servers.2]
  host = "127.0.0.1"
  port = "3306"
  user = "ch"
  password = "123456"
  name = "ch2"
```

填写正确的数据库连接, schemaname 名称与 serers name 名称对应一致。

go run main.go

# log 日志输出到文件中，配置方法

```
// 定义一个日志
logFile, err := os.Create("diff.log")
defer logFile.Close()
if err != nil {
	log.Fatalln("open file error !")
}
// 创建一个日志对象
dLog = log.New(logFile, "[Info]", log.LstdFlags|log.Lshortfile)
//配置一个日志格式的前缀
dLog.SetPrefix("[Info]")
//配置log的Flag参数
dLog.SetFlags(dLog.Flags() | log.LstdFlags)
```
