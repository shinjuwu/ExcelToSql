package main

import (
	"ExcelToSql/base"
	"ExcelToSql/conf"
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tealeg/xlsx"
	"golang.org/x/crypto/bcrypt"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	c := new(base.Columns)
	// if len(os.Args) != 4 {
	// 	fmt.Println("請輸入格式:exceltomysql [DSN] [資料庫名稱] [*.xlsx]")
	// 	os.Exit(-1)
	// }

	// dsn := os.Args[1]
	tableName := os.Args[1]
	//fileName := os.Args[2]
	var dsn string = conf.Setting.Dsn
	//var tableName string = "account_vip"
	var fileName string = conf.Setting.FilePath + os.Args[2]

	db, err := sql.Open("mysql", dsn)
	base.Checkerr(err)
	defer db.Close()
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)

	rows, err := db.Query("SELECT * FROM " + tableName + " LIMIT 1")
	base.Checkerr(err)

	c.TableColumns, err = rows.Columns()
	base.Checkerr(err)
	rows.Close()

	xlFile, err := xlsx.OpenFile(fileName)
	base.Checkerr(err)

	c.XlsxColumns = xlFile.Sheets[0].Rows[0].Cells

	c.ParaseColumns()
	//ch := make(chan int, 50)
	//sign := make(chan string, len(xlFile.Sheets[0].Rows))
	rowsnum := len(xlFile.Sheets[0].Rows)
	for i := 1; i < rowsnum; i++ {
		//ch <- i
		//go func(i int) {
		tx, err := db.Begin()
		defer tx.Rollback()
		base.Checkerr(err)
		r := &base.Row{Value: make(map[string]string), Sql: "INSERT INTO `" + tableName + "` SET ", Ot: base.OtherTable{}}
		tmp := 0
		for k, v := range c.UseColumns {
			if len(v) == 0 {
				continue
			}
			r.Value[v[0]] = xlFile.Sheets[0].Rows[i].Cells[k].String()

			//parse the context
			if v[0] == ":other" {
				r.Ot.Value = strings.Split(r.Value[v[0]], "|")
				rows, err := db.Query("SELECT * FORM " + r.Ot.Value[0])
				base.Checkerr(err)
				r.Ot.Columns, err = rows.Columns()
				rows.Close()
				base.Checkerr(err)
			} else {
				if len(v) > 1 {
					switch v[1] {
					case "unique":
						result, _ := base.FetchRow(db, "SELECT count("+v[0]+") as has FROM `"+tableName+"` WHERE `"+v[0]+"` = `"+r.Value[v[0]]+"`")
						has, _ := strconv.Atoi((*result)["has"])
						if has > 0 {
							fmt.Println("[" + strconv.Itoa(i) + "/" + strconv.Itoa(rowsnum-1) + "]" + v[0] + ":" + r.Value[v[0]] + "重複,自動跳過\n")
							//sign <- "error"
							return
						}
					case "password":
						tmpvalue := strings.Split(r.Value[v[0]], "|")
						if len(tmpvalue) == 2 {
							if []byte(tmpvalue[1])[0] == ':' {
								if _, ok := r.Value[string([]byte(tmpvalue[1])[1:])]; ok {
									r.Value[v[0]] = tmpvalue[0] + r.Value[string([]byte(tmpvalue[1])[1:])]
								} else {
									fmt.Println("[" + strconv.Itoa(i) + "/" + strconv.Itoa(rowsnum-1) + "]密碼鹽" + string([]byte(tmpvalue[1])[1:]) + "字段不存在，自動跳過\n")
									//sign <- "error"
									return
								}
							} else {
								r.Value[v[0]] += tmpvalue[1]
							}
						} else {
							r.Value[v[0]] = tmpvalue[0]
						}

						switch v[2] {
						case "md5":
							r.Value[v[0]] = string(md5.New().Sum([]byte(r.Value[v[0]])))
						case "bcrypt":
							pass, _ := bcrypt.GenerateFromPassword([]byte(r.Value[v[0]]), 13)
							r.Value[v[0]] = string(pass)
						}
					case "find":
						result, _ := base.FetchRow(db, "SELECT `"+v[3]+"` FROM `"+v[2]+"` WHERE "+v[4]+" = `"+r.Value[v[0]]+"`")
						if (*result)["id"] == "" {
							fmt.Print("[" + strconv.Itoa(i) + "/" + strconv.Itoa(rowsnum-1) + "]表" + v[2] + " 中沒有找到 " + v[4] + " 為 " + r.Value[v[0]] + " 的數據,自動跳過\n")
							//sign <- "error"
							return
						}
						r.Value[v[0]] = (*result)["id"]
					}
				}
				r.Value[v[0]] = base.ParseValue(r.Value[v[0]])
				if r.Value[v[0]] != "" {
					if tmp == 0 {
						r.Sql += "`" + v[0] + "` ='" + r.Value[v[0]] + "'"
					} else {
						r.Sql += ", `" + v[0] + "` ='" + r.Value[v[0]] + "'"
					}
					tmp++
				}
			}
		}

		smt, err := tx.Prepare(r.Sql + ";")
		fmt.Printf("SQL: %s", r.Sql+";")
		base.Checkerr(err)
		res, err := smt.Exec()
		r.InsertID, err = res.LastInsertId()
		base.Checkerr(err)
		smt.Close()

		//執行附表操作
		if r.Ot.Value != nil {
			r.Ot.Sql = "INSERT INTO '" + r.Ot.Value[0] + "' SET "
			tmp = 0
			for k, v := range r.Ot.Columns {
				r.Ot.Value[k+1] = base.ParseValue(r.Ot.Value[k+1])
				if r.Ot.Value[k+1] == ":id" {
					r.Ot.Value[k+1] = strconv.Itoa(int(r.InsertID))
				}
				if r.Ot.Value[k+1] != "" {
					if tmp == 0 {
						r.Ot.Sql += "`" + v + "` = '" + r.Ot.Value[k+1] + "'"
					} else {
						r.Ot.Sql += ", `" + v + "` = '" + r.Ot.Value[k+1] + "'"

					}
					tmp++
				}
			}
			otsmt, err := tx.Prepare(r.Ot.Sql + ";")
			base.Checkerr(err)
			_, err = otsmt.Exec()
			base.Checkerr(err)
			otsmt.Close()
		}
		err = tx.Commit()
		base.Checkerr(err)
		fmt.Println("[" + strconv.Itoa(i) + "/" + strconv.Itoa(rowsnum-1) + "]導入數據成功")
		//sign <- "success"
		//}(i)
	}
	// for i := 1; i < rowsnum; i++ {
	// 	<-sign
	// }
}
