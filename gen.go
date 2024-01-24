package main

import (
	"encoding/csv"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"os"
	"strconv"
	"sync"
	"time"
)

type Row struct {
	ID          int
	ProductName string
	Price       float64
	Quantity    int
	Discount    float64
	TotalPrice  float64
	CustomerID  int
	FirstName   string
	LastName    string
	Email       string
	Address     string
	City        string
	State       string
	Country     string
}

func GenerateData(numRows int, wg *sync.WaitGroup, ch chan<- Row) {
	defer wg.Done()

	startTime := time.Now()

	for i := 0; i < numRows; i++ {
		price := gofakeit.Price(10, 200)
		discount := gofakeit.Float64Range(0, 0.2)

		row := Row{
			ID:          i + 1,
			ProductName: gofakeit.Name(),
			Price:       price,
			Quantity:    gofakeit.Number(1, 10),
			Discount:    discount,
			TotalPrice:  price * (1 - discount),
			CustomerID:  gofakeit.Number(1000, 9999),
			FirstName:   gofakeit.FirstName(),
			LastName:    gofakeit.LastName(),
			Email:       gofakeit.Email(),
			Address:     gofakeit.Address().Address,
			City:        gofakeit.City(),
			State:       gofakeit.State(),
			Country:     gofakeit.Country(),
		}

		ch <- row
	}

	close(ch)

	elapsedTime := time.Since(startTime)
	fmt.Printf("Data generation took %s\n", elapsedTime)
}

func WriteToCSV(filename string, ch <-chan Row, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"id", "product_name", "price", "quantity", "discount", "total_price",
		"customer_id", "first_name", "last_name", "email", "address", "city", "state", "country",
	}
	if err := writer.Write(header); err != nil {
		fmt.Println("Error writing header:", err)
		return
	}

	for row := range ch {
		record := []string{
			strconv.Itoa(row.ID),
			row.ProductName,
			fmt.Sprintf("%.2f", row.Price),
			strconv.Itoa(row.Quantity),
			fmt.Sprintf("%.2f", row.Discount),
			fmt.Sprintf("%.2f", row.TotalPrice),
			strconv.Itoa(row.CustomerID),
			row.FirstName,
			row.LastName,
			row.Email,
			row.Address,
			row.City,
			row.State,
			row.Country,
		}
		if err := writer.Write(record); err != nil {
			fmt.Println("Error writing record:", err)
			return
		}
	}
}

func main() {
	gofakeit.Seed(time.Now().UnixNano())

	numRows := 30000000
	outputFilename := "ecommerce_data.csv"

	ch := make(chan Row, 1000)

	var wg sync.WaitGroup

	wg.Add(1)
	go GenerateData(numRows, &wg, ch)

	wg.Add(1)
	go WriteToCSV(outputFilename, ch, &wg)

	wg.Wait()

	fmt.Printf("Generated %d rows of e-commerce data and saved to %s\n", numRows, outputFilename)
}

