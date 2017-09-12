package exmysql

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

// 得到mysql数据库连接对象
func GetDBConn(dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return db, err
}

type AsmGeo struct {
	GeoId       string
	Entity      string
	DisplayName string
	CreateTime  int
}

/* func InsertStruct(db *sql.DB, o interface{}) {
	stmt, err := db.Prepare("INSERT INTO ASM_GEO2(geoId, entity, displayName, createTime) VALUES(?, ?, ?, ?)")
	// defer stmt.Close()
	if err != nil {
		log.Println(err)
		return
	}

	t := reflect.TypeOf(o)
	v := reflect.ValueOf(o)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		val := v.Field(i).Interface()
		fmt.Printf("%6s : %v %v\n", f.Name, f.Type, val)
	}

	stmt.Exec(ag.GeoId, ag.Entity, ag.DisplayName, ag.CreateTime)
}*/

//  数据插入方法
func Insert(db *sql.DB, sql string) {
	stmt, err := db.Prepare(sql)
	// defer stmt.Close()
	if err != nil {
		log.Println(err)
		return
	}
	stmt.Exec()
}

//  数据更新数据库方法
func UpdateMysql(db *sql.DB, sql string) {
	stmt, err := db.Prepare(sql)
	// defer stmt.Close()
	if err != nil {
		log.Println(err)
		return
	}
	stmt.Exec()
}

// 向数据库表中查询1列，或者count语句
func GetColumn1(db *sql.DB, sql string) []string {
	values := []string{}
	rows, err := db.Query(sql)
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	var value string
	for rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			log.Fatal(err)
		}
		values = append(values, value)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return values
}
