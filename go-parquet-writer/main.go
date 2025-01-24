package main

import (
	"fmt"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
)

func main() {

	type Row struct {
		City         string `parquet:"city,string"`
		Country      string `parquet:"country,string"`
		Age          uint8  `parquet:"age"`
		Scale        int16  `parquet:"scale"`
		Status       uint32 `parquet:"status"`
		TimeCaptured int64  `parquet:"time_captured,timestamp"`
		Checked      bool   `parquet:"checked"`
	}

	file, err := os.Create("go-testfile.parquet")
	if err != nil {
		panic(fmt.Errorf("failed to create go-testfile.parquet: %v", err))
	}

	schema := parquet.SchemaOf(new(Row))
	writer := parquet.NewGenericWriter[any](file, schema)
	rows := []Row{
		{
			City:         "Madrid",
			Country:      "Spain",
			Age:          10,
			Scale:        -1,
			Status:       12,
			TimeCaptured: time.Now().UTC().UnixMilli(),
			Checked:      false,
		},
		{
			City:         "Athens",
			Country:      "Greece",
			Age:          32,
			Scale:        1,
			Status:       20,
			TimeCaptured: time.Now().UTC().Add(1 * time.Hour).UnixMilli(),
			Checked:      true,
		},
	}

	input := make([]any, len(rows))
	for i, r := range rows {
		input[i] = r
	}
	_, err = writer.Write(input)
	if err != nil {
		panic(fmt.Errorf("failed to write rows to parquet file: %v", err))
	}

	err = writer.Close()
	if err != nil {
		panic(fmt.Errorf("failed to close parquet writer: %v", err))
	}
}
