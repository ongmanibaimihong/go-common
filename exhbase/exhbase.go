//
// Golang基于Hbase-1.2.2的Thrift2访问Hbase
//
// @author     songhq
// @copyright  Copyright (c) 2016
// @license    GNU General Public License 2.0
// @version    1

package exhbase

import (
	"encoding/binary"
	"fmt"
	"hbase"
	"net"
	"os"
	"reflect"
	"strconv"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var hourList = [24]string{"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"}
var TESTRECORD = 10

// 获取hbase连接对象
func GetHbaseClient(host string, port string) *hbase.THBaseServiceClient {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transport, err := thrift.NewTSocket(net.JoinHostPort(host, port))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error resolving address:", err) // 解析地址错误
		os.Exit(1)
	}
	client := hbase.NewTHBaseServiceClientFactory(transport, protocolFactory)
	if err := transport.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening socket to "+host+":"+port, " ", err)
		os.Exit(1)
	}
	return client
}

// 判断rowkey是否存在
func Exists(client *hbase.THBaseServiceClient, tableName string, rowkey string) bool {
	isexists, err := (client.Exists([]byte(tableName), &hbase.TGet{Row: []byte(rowkey)}))
	fmt.Printf("rowkey{%s} in table{%s} Exists:%t\t", rowkey, tableName, isexists)
	if err != nil {
		fmt.Printf("Exists err:%s\n", err)
	}
	return isexists
}

// 调用Get方法获取数据
func Get(client *hbase.THBaseServiceClient, tableName string, rowkey string) {
	result, err := (client.Get([]byte(tableName), &hbase.TGet{Row: []byte(rowkey)}))
	if err != nil {
		fmt.Printf("Get err:%s\n", err)
	} else {
		fmt.Println("Rowkey:" + string(result.Row))
		for _, cv := range result.ColumnValues {
			PrintScruct(cv)
		}
	}
}

// 调用DeleteSingle方法删除一条数据
func DeleteSingle(client *hbase.THBaseServiceClient, tableName string, rowkey string) {
	tdelete := hbase.TDelete{Row: []byte(rowkey)}
	err := client.DeleteSingle([]byte(tableName), &tdelete)
	if err != nil {
		fmt.Printf("DeleteSingle err:%s\n", err)
	} else {
		fmt.Printf("DeleteSingel done\n")
	}
}

// FilterString: []byte("PrefixFilter('1407658495588-')"),
// scan 'userbt',{FILTER => "PrefixFilter ('8')"}
// scan 'aso_apptop_info_free',{FILTER => "PrefixFilter('995999703_')"}
func GetScannerResults(client *hbase.THBaseServiceClient, tableName string, startrow string, stoprow string, date string) []*hbase.TResult_ {
	gsr, err := client.GetScannerResults([]byte(tableName), &hbase.TScan{
		StartRow:     []byte(startrow),
		StopRow:      []byte(stoprow),
		FilterString: []byte("ColumnPrefixFilter('" + date + "')"),
		Columns: []*hbase.TColumn{
			&hbase.TColumn{
				Family:    []byte("kt"),
				Qualifier: []byte("2017-09-12 08")},
			&hbase.TColumn{
				Family:    []byte("kt"),
				Qualifier: []byte("2017-09-12 09")}}}, 10)
	if err != nil {
		fmt.Printf("GetScannerResults err:%s\n", err.Error())
	} else {
		fmt.Printf("GetScannerResults %d done\n", len(gsr))
	}
	return gsr
}

func OpenScanner(client *hbase.THBaseServiceClient, tableName string, family string, startrow string, stoprow string, date string, numRows int32) []*hbase.TResult_ {
	var scanresult []*hbase.TResult_
	var hts []*hbase.TColumn
	for i := range hourList {
		ht := &hbase.TColumn{
			Family:    []byte(family),
			Qualifier: []byte(date + " " + hourList[i])}
		hts = append(hts, ht)
	}
	scanresultnum, err := client.OpenScanner([]byte(tableName), &hbase.TScan{
		StartRow:     []byte(startrow),
		StopRow:      []byte(stoprow),
		FilterString: []byte("ColumnPrefixFilter('" + date + "')"),
		Columns:      hts})
	if err != nil {
		fmt.Printf("OpenScanner err:%s\n", err)
	} else {
		fmt.Printf("OpenScanner %d done\n", scanresultnum)

		scanresult, err = client.GetScannerRows(scanresultnum, numRows)
		if err != nil {
			fmt.Printf("GetScannerRows err:%s\n", err)
		} else {
			fmt.Printf("GetScannerRows %d done\n", len(scanresult))
		}
	}

	defer client.CloseScanner(scanresultnum)

	return scanresult
}

func PrintScruct(cv interface{}) {
	switch reflect.ValueOf(cv).Interface().(type) {
	case *hbase.TColumnValue:
		s := reflect.ValueOf(cv).Elem()
		typeOfT := s.Type()
		//获取Thrift2中struct的field
		for i := 0; i < s.NumField(); i++ {
			f := s.Field(i)
			fileldformatstr := "\t%d: %s(%s)= %v\n"
			switch f.Interface().(type) {
			case []uint8:
				fmt.Printf(fileldformatstr, i, typeOfT.Field(i).Name, f.Type(), string(f.Interface().([]uint8)))
			case *int64:
				var tempint64 int64
				if f.Interface().(*int64) == nil {
					tempint64 = 0
				} else {
					tempint64 = *f.Interface().(*int64)
				}
				fmt.Printf(fileldformatstr, i, typeOfT.Field(i).Name, f.Type(), tempint64)
			default:
				fmt.Printf("I don't know")
			}
		}
	default:
		fmt.Printf("I don't know")
		fmt.Print(reflect.ValueOf(cv))
	}
}

func main() {
	logformatstr_ := "----%s\n"
	logformatstr := "----%s 用时:%d-%d=%d毫秒\n\n"
	logformattitle := "建立连接"
	rowkey := "8888"
	temptable := "userbt"

	client := GetHbaseClient("hadoop98", "9090")
	defer client.Transport.Close()

	//--------------put
	logformattitle = "调用Put方法写数据"
	fmt.Printf(logformatstr_, logformattitle)
	tmpstartTime := currentTimeMillis()
	cvarr := []*hbase.TColumnValue{
		&hbase.TColumnValue{
			Family:    []byte("upf"),
			Qualifier: []byte("intt"),
			Value:     []byte("100")},
		&hbase.TColumnValue{
			Family:    []byte("upf"),
			Qualifier: []byte("intt"),
			Value:     []byte("110")}}
	temptput := hbase.TPut{Row: []byte(rowkey), ColumnValues: cvarr}
	err := client.Put([]byte(temptable), &temptput)
	if err != nil {
		fmt.Printf("Put err:%s\n", err)
	} else {
		fmt.Println("Put done")
	}
	tmpendTime := currentTimeMillis()
	fmt.Printf(logformatstr, logformattitle, tmpendTime, tmpstartTime, (tmpendTime - tmpstartTime))

	//------------Get---------------

	//--------------put update
	logformattitle = "调用Put update方法'修改'数据"
	fmt.Printf(logformatstr_, logformattitle)
	tmpstartTime = currentTimeMillis()
	cvarr = []*hbase.TColumnValue{
		&hbase.TColumnValue{
			Family:    []byte("name"),
			Qualifier: []byte("idoall.org"),
			Value:     []byte("welcome idoall.org---update")}}
	temptput = hbase.TPut{Row: []byte(rowkey), ColumnValues: cvarr}
	err = client.Put([]byte(temptable), &temptput)
	if err != nil {
		fmt.Printf("Put update err:%s\n", err)
	} else {
		fmt.Println("Put update done")
	}
	tmpendTime = currentTimeMillis()
	fmt.Printf(logformatstr, logformattitle, tmpendTime, tmpstartTime, (tmpendTime - tmpstartTime))

	//------------Get update---------------
	logformattitle = "调用Get方法获取'修改'后的数据"
	fmt.Printf(logformatstr_, logformattitle)
	tmpstartTime = currentTimeMillis()
	result, err := (client.Get([]byte(temptable), &hbase.TGet{Row: []byte(rowkey)}))
	if err != nil {
		fmt.Printf("Get update err:%s\n", err)
	} else {
		fmt.Println("update Rowkey:" + string(result.Row))
		for _, cv := range result.ColumnValues {
			PrintScruct(cv)
		}
	}
	tmpendTime = currentTimeMillis()
	fmt.Printf(logformatstr, logformattitle, tmpendTime, tmpstartTime, (tmpendTime - tmpstartTime))

	//------------DeleteSingle------------

	//-------------PutMultiple----------------
	logformattitle = "调用PutMultiple方法添加" + strconv.Itoa(TESTRECORD) + "条数据"
	fmt.Printf(logformatstr_, logformattitle)
	tmpstartTime = currentTimeMillis()
	var tputArr []*hbase.TPut
	for i := 0; i < TESTRECORD; i++ {
		putrowkey := strconv.Itoa(i)
		tputArr = append(tputArr, &hbase.TPut{
			Row: []byte(putrowkey),
			ColumnValues: []*hbase.TColumnValue{
				&hbase.TColumnValue{
					Family:    []byte("upf"),
					Qualifier: []byte("intt"),
					Value:     []byte(time.Now().String())}}})
	}
	err = client.PutMultiple([]byte(temptable), tputArr)
	if err != nil {
		fmt.Printf("PutMultiple err:%s\n", err)
	} else {
		fmt.Printf("PutMultiple done\n")
	}
	tmpendTime = currentTimeMillis()
	fmt.Printf(logformatstr, logformattitle, tmpendTime, tmpstartTime, (tmpendTime - tmpstartTime))

	//------------------GetMultiple-----------------------------
	logformattitle = "调用GetMultiple方法获取" + strconv.Itoa(TESTRECORD) + "数据"
	fmt.Printf(logformatstr_, logformattitle)
	tmpstartTime = currentTimeMillis()
	var tgets []*hbase.TGet
	for i := 0; i < TESTRECORD; i++ {
		putrowkey := strconv.Itoa(i)
		tgets = append(tgets, &hbase.TGet{
			Row: []byte(putrowkey)})
	}
	results, err := client.GetMultiple([]byte(temptable), tgets)
	if err != nil {
		fmt.Printf("GetMultiple err:%s", err)
	} else {
		fmt.Printf("GetMultiple Count:%d\n", len(results))
		for _, k := range results {
			fmt.Println("Rowkey:" + string(k.Row))
			for _, cv := range k.ColumnValues {
				PrintScruct(cv)
			}
		}
	}
	tmpendTime = currentTimeMillis()
	fmt.Printf(logformatstr, logformattitle, tmpendTime, tmpstartTime, (tmpendTime - tmpstartTime))
	//-------------------TMutation
	//TMutation包含一个TGet一个TPut，就不做测试了
	//可以和MutateRow结合使用

	//-------------------OpenScanner
	startrow := make([]byte, 4)
	binary.LittleEndian.PutUint32(startrow, 1)
	stoprow := make([]byte, 4)
	binary.LittleEndian.PutUint32(stoprow, 10)

	//--closescanner

	//-------------------GetScannerResults

	//---------------DeleteMultiple--------------
	/*logformattitle = "调用DeleteMultiple方法删除" + strconv.Itoa(TESTRECORD) + "数据"
	fmt.Printf(logformatstr_, logformattitle)
	tmpstartTime = currentTimeMillis()
	var tdelArr []*hbase.TDelete
	for i := 0; i < TESTRECORD; i++ {
		putrowkey := strconv.Itoa(i)
		tdelArr = append(tdelArr, &hbase.TDelete{
			Row: []byte(putrowkey)})
	}
	r, err := client.DeleteMultiple([]byte(temptable), tdelArr)
	if err != nil {
		fmt.Printf("DeleteMultiple err:%s\n", err)
	} else {
		fmt.Printf("DeleteMultiple %d done\n", TESTRECORD)
		fmt.Println(r)
	}
	tmpendTime = currentTimeMillis()
	fmt.Printf(logformatstr, logformattitle, tmpendTime, tmpstartTime, (tmpendTime - tmpstartTime))

	endTime := currentTimeMillis()
	fmt.Printf("\nGolang调用总计用时:%d-%d=%d毫秒\n", endTime, startTime, (endTime - startTime))*/
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}
