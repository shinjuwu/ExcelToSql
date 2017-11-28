package base

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tealeg/xlsx"
)

type Columns struct {
	XlsxColumns  []*xlsx.Cell
	TableColumns []string
	UseColumns   [][]string
}

type Row struct {
	InsertID int64
	Sql      string
	Value    map[string]string
	Ot       OtherTable
}

type OtherTable struct {
	Sql     string
	Value   []string
	Columns []string
}

func (c *Columns) ParaseColumns() {
	c.UseColumns = make([][]string, len(c.XlsxColumns))
	for k, v := range c.XlsxColumns {
		thisCol := v.String()
		colval := strings.Split(thisCol, "|")
		if colval[0] == ":other" {
			c.UseColumns[k] = colval
		} else {
			for _, value := range c.TableColumns {
				if colval[0] == value {
					c.UseColumns[k] = colval
				}
			}
		}
	}

	if len(c.UseColumns) < 1 {
		fmt.Println("資料庫表與excel表不對應")
		os.Exit(-1)
	}
}

func ParseValue(val string) string {
	switch val {
	case ":null":
		return ""
	case ":time":
		return strconv.Itoa(int(time.Now().Unix()))
	case ":random":
		return strings.Replace(substr(base64.StdEncoding.EncodeToString(Krand(32, KC_RAND_KIND_ALL)), 0, 32), "+/", "_-", -1)
	default:
		return val
	}
}

func substr(str string, start int, length int) string {
	rs := []rune(str)
	r1 := len(rs)
	end := 0

	if start < 0 {
		start = r1 - 1 + start
	}
	end = start + length

	if start > end {
		start, end = end, start
	}

	if start < 0 {
		start = 0
	}

	if start > r1 {
		start = r1
	}

	if end < 0 {
		end = 0
	}

	if end > r1 {
		end = r1
	}

	return string(rs[start:end])
}
