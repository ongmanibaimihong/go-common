package main

import (
	"fmt"

	"github.com/songhuiqing/go-common/exhbase"
	// "github.com/songhuiqing/go-common/exhbase/hbase"
)

func main() {
	client := exhbase.GetHbaseClient("hadoop100", "9090")
	fmt.Println("建立连接")
	// exhbase.Get(client, "aso_appkw_top", "351091731_5267606")
	// exhbase.GetScannerResults(client, "testImport1", "88", "89", "intt")
	// exhbase.GetScannerResults(client, "aso_appkw_top", "351091731", "351091732", "2017-09-12")
	scanresult := exhbase.OpenScanner(client, "aso_appkw_top", "kt", "351091731", "351091732", "2017-09-11", 20)
	for _, k := range scanresult {
		fmt.Println("scan Rowkey:" + string(k.Row))
		for _, cv := range k.ColumnValues {
			exhbase.PrintScruct(cv)
		}
	}
	defer client.Transport.Close()
}
