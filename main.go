package main

import (
	"fmt"
	"log"
	"os"

	"database/sql"

	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
)

type tomlConfig struct {
	SchemaName1 string
	SchemaName2 string
	Servers     map[string]database
}

type database struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
}

var (
	driverName string
	dbConfig   tomlConfig
	dLog       *log.Logger
)

func init() {
	driverName = "mysql"
}

// Conn 连接数据库
func Conn(dataSourceName string) *sql.DB {
	//连接数据库
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		dLog.Println(err.Error())
		os.Exit(-1)
	}
	if err := db.Ping(); err != nil {
		panic("ERROR:" + err.Error())
	}
	return db
}

func getSource(db string) (source string) {
	// dataSourceName = "用户名:密码@tcp(localhost:3306)/数据库名称?charset=utf8"
	source = dbConfig.Servers[db].User +
		":" +
		dbConfig.Servers[db].Password +
		"@tcp(" +
		dbConfig.Servers[db].Host +
		":" +
		dbConfig.Servers[db].Port +
		")/" +
		dbConfig.Servers[db].Name +
		"?charset=utf8"
	return
}

func main() {

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
	dLog.Println("开始执行……")
	// 读取配置文件
	if _, err := toml.DecodeFile("config.toml", &dbConfig); err != nil {
		fmt.Println("解析错误:", err.Error())
		return
	}

	// 获得一个sql连接
	dLog.Printf("连接%s/%s数据库……", dbConfig.Servers["1"].Host, dbConfig.Servers["1"].Name)
	schema1 := dbConfig.SchemaName1
	db1 := Conn(getSource("1"))
	defer db1.Close()

	// 获得第二个sql连接
	dLog.Printf("连接%s/%s数据库……", dbConfig.Servers["2"].Host, dbConfig.Servers["2"].Name)
	schema2 := dbConfig.SchemaName2
	db2 := Conn(getSource("2"))
	defer db2.Close()

	// 对比触发器
	dLog.Println("对比触发器……")
	TriggerDiff(db1, db2, schema1, schema2)
	// 对比函数
	dLog.Println("对比函数……")
	FunctionDiff(db1, db2, schema1, schema2)
	// 对比表
	dLog.Println("对比表……")
	ts, b := TableDiff(db1, db2, schema1, schema2)
	if b {
		// 对比列名
		dLog.Println("对比列名……")
		ColumnDiff(db1, db2, schema1, schema2, ts)
		// 对比索引
		dLog.Println("对比索引……")
		IndexDiff(db1, db2, schema1, schema2, ts)
	}

	dLog.Println("执行结束……")
}

// TableDiff 对比表的不同
func TableDiff(db1, db2 *sql.DB, schema1, schema2 string) (t []string, b bool) {
	tableName1, err := getTableName(db1, schema1)
	if err != nil {
		dLog.Fatalln(err.Error())
	}

	dLog.Println(dbConfig.Servers["1"].Host, "/", schema1, " 表名： ", tableName1)
	tableName2, err := getTableName(db2, schema2)
	if err != nil {
		dLog.Fatalln(err.Error())
	}

	dLog.Println(dbConfig.Servers["2"].Host, "/", schema2, " 表名： ", tableName2)
	if !isEqual(tableName1, tableName2) {
		t = diffName(tableName1, tableName2)
		dLog.Printf("两个数据库不同的表,共有%d个，分别是：%s", len(t), t)
		return t, false
	}
	t = tableName1
	dLog.Printf("两个数据库表相同")
	return t, true
}

func getTableName(s *sql.DB, table string) (ts []string, err error) {
	stm, perr := s.Prepare("select table_name from information_schema.tables where table_schema=? order by table_name")
	if perr != nil {
		err = perr
		return
	}
	defer stm.Close()
	q, qerr := stm.Query(table)
	if qerr != nil {
		err = qerr
		return
	}
	defer q.Close()

	for q.Next() {
		var name string
		if err := q.Scan(&name); err != nil {
			log.Fatal(err)
		}
		ts = append(ts, name)
	}
	return
}

// TriggerDiff 对比触发器的不同
func TriggerDiff(db1, db2 *sql.DB, schema1, schema2 string) bool {
	triggerName1, err := getTriggerName(db1, schema1)
	if err != nil {
		dLog.Fatalln(err.Error())
	}
	triggerName2, err := getTriggerName(db2, schema2)
	if err != nil {
		dLog.Fatalln(err.Error())
	}
	if !isEqual(triggerName1, triggerName2) {
		dt := diffName(triggerName1, triggerName2)
		dLog.Printf("两个数据库不同的触发器,共有%d个，分别是：%s", len(dt), dt)
		return false
	}
	dLog.Printf("两个数据库触发器相同")
	return true
}

func getTriggerName(s *sql.DB, schema string) (ts []string, err error) {
	stm, perr := s.Prepare("select TRIGGER_NAME from information_schema.triggers where TRIGGER_SCHEMA=? order by TRIGGER_NAME")
	if perr != nil {
		err = perr
		return
	}
	defer stm.Close()
	q, qerr := stm.Query(schema)
	if qerr != nil {
		err = qerr
		return
	}
	defer q.Close()

	for q.Next() {
		var name string
		if err := q.Scan(&name); err != nil {
			log.Fatal(err)
		}
		ts = append(ts, name)
	}
	return
}

// FunctionDiff 对比函数的不同
func FunctionDiff(db1, db2 *sql.DB, schema1, schema2 string) bool {
	functionName1, err := getFunctionName(db1, schema1)
	if err != nil {
		dLog.Fatalln(err.Error())
	}
	functionName2, err := getFunctionName(db2, schema2)
	if err != nil {
		dLog.Fatalln(err.Error())
	}
	dLog.Println(functionName1)
	dLog.Println(functionName2)
	if !isEqual(functionName1, functionName2) {
		dt := diffName(functionName1, functionName2)
		dLog.Printf("两个数据库不同的函数,共有%d个，分别是：%s", len(dt), dt)
		return false
	}
	dLog.Printf("两个数据库函数相同")
	return true
}

func getFunctionName(s *sql.DB, schema string) (ts []string, err error) {
	stm, perr := s.Prepare("select ROUTINE_NAME from information_schema.routines where ROUTINE_SCHEMA=? and ROUTINE_TYPE='FUNCTION' order by ROUTINE_NAME")
	if perr != nil {
		err = perr
		return
	}
	defer stm.Close()
	q, qerr := stm.Query(schema)
	if qerr != nil {
		err = qerr
		return
	}
	defer q.Close()

	for q.Next() {
		var name string
		if err := q.Scan(&name); err != nil {
			log.Fatal(err)
		}
		ts = append(ts, name)
	}
	return
}

// ColumnDiff 对比函数的不同
func ColumnDiff(db1, db2 *sql.DB, schema1, schema2 string, table []string) {
	for _, t := range table {
		columnName1, err := getColumnName(db1, schema1, t)
		if err != nil {
			dLog.Fatalln(err.Error())
		}
		columnName2, err := getColumnName(db2, schema2, t)
		if err != nil {
			dLog.Fatalln(err.Error())
		}
		if !isEqual(columnName1, columnName2) {
			dt := diffName(columnName1, columnName2)
			dLog.Printf("两个数据库%s表，有不同的列,共有%d个，分别是：%s", t, len(dt), dt)
		} else {
			dLog.Printf("两个数据库%s表，列相同", t)
		}
	}
}

func getColumnName(s *sql.DB, schema, table string) (ts []string, err error) {
	stm, perr := s.Prepare("select COLUMN_NAME from information_schema.columns where TABLE_SCHEMA=? and TABLE_NAME=? order by COLUMN_NAME")
	if perr != nil {
		err = perr
		return
	}
	defer stm.Close()
	q, qerr := stm.Query(schema, table)
	if qerr != nil {
		err = qerr
		return
	}
	defer q.Close()

	for q.Next() {
		var name string
		if err := q.Scan(&name); err != nil {
			log.Fatal(err)
		}
		ts = append(ts, name)
	}
	return
}

// IndexDiff 对比函数的不同
func IndexDiff(db1, db2 *sql.DB, schema1, schema2 string, table []string) {
	for _, t := range table {
		indexName1, err := getIndexName(db1, schema1, t)
		if err != nil {
			dLog.Fatalln(err.Error())
		}
		indexName2, err := getIndexName(db2, schema2, t)
		if err != nil {
			dLog.Fatalln(err.Error())
		}
		if !isEqual(indexName1, indexName2) {
			dt := diffName(indexName1, indexName2)
			dLog.Printf("两个数据库%s表，有不同的索引,共有%d个，分别是：%s", t, len(dt), dt)
		} else {
			dLog.Printf("两个数据库%s表，索引相同", t)
		}
	}
}

func getIndexName(s *sql.DB, schema, table string) (ts []string, err error) {
	stm, perr := s.Prepare("select INDEX_NAME from information_schema.STATISTICS where TABLE_SCHEMA=? and TABLE_NAME=? order by INDEX_NAME")
	if perr != nil {
		err = perr
		return
	}
	defer stm.Close()
	q, qerr := stm.Query(schema, table)
	if qerr != nil {
		err = qerr
		return
	}
	defer q.Close()

	for q.Next() {
		var name string
		if err := q.Scan(&name); err != nil {
			log.Fatal(err)
		}
		ts = append(ts, name)
	}
	return
}

func isEqual(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func diffName(a, b []string) []string {
	c := a[:0]
	m := make(map[string]int)
	for _, s := range a {
		m[s] = 1
	}

	// 交集
	n := make(map[string]int)
	for _, s := range b {
		if _, ok := m[s]; ok {
			n[s] = 1
		}
	}
	// 与a不同
	for _, s := range a {
		if _, ok := n[s]; !ok {
			c = append(c, s)
		}
	}
	// 与b不同
	for _, s := range b {
		if _, ok := n[s]; !ok {
			c = append(c, s)
		}
	}
	return c
}
