/*
Name: Mehmet Taha Ünal
Student ID: 231AMB077
Assignment: Concurrent Chunk Sorting (gosort)
*/

package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* -------------------- MAIN -------------------- */

func main() {
	r := flag.Int("r", -1, "generate N random integers (N >= 10)")
	i := flag.String("i", "", "input file with integers")
	flag.Parse()

	if *r != -1 {
		if err := runRandom(*r); err != nil {
			log.Fatal(err)
		}
		return
	}

	if *i != "" {
		if err := runInputFile(*i); err != nil {
			log.Fatal(err)
		}
		return
	}

	log.Fatal("usage: gosort -r N | gosort -i file.txt")
}

/* -------------------- -r MODE -------------------- */

func runRandom(n int) error {
	if n < 10 {
		return errors.New("n must be >= 10")
	}

	rand.Seed(time.Now().UnixNano())
	numbers := make([]int, n)
	for i := range numbers {
		numbers[i] = rand.Intn(1000) // 0–999
	}

	fmt.Println("Original numbers:")
	fmt.Println(numbers)

	processAndPrint(numbers)
	return nil
}

/* -------------------- -i MODE -------------------- */

func runInputFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var numbers []int
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		v, err := strconv.Atoi(line)
		if err != nil {
			return fmt.Errorf("invalid integer: %s", line)
		}
		numbers = append(numbers, v)
	}

	if len(numbers) < 10 {
		return errors.New("file must contain at least 10 integers")
	}

	fmt.Println("Original numbers:")
	fmt.Println(numbers)

	processAndPrint(numbers)
	return nil
}

/* -------------------- COMMON PIPELINE -------------------- */

func processAndPrint(numbers []int) {
	chunks := splitIntoChunks(numbers)

	fmt.Println("\nChunks before sorting:")
	printChunks(chunks)

	sortChunksConcurrently(chunks)

	fmt.Println("\nChunks after sorting:")
	printChunks(chunks)

	result := mergeSortedChunks(chunks)

	fmt.Println("\nFinal sorted result:")
	fmt.Println(result)
}

/* -------------------- CHUNKING -------------------- */

func splitIntoChunks(numbers []int) [][]int {
	n := len(numbers)

	chunkCount := int(math.Ceil(math.Sqrt(float64(n))))
	if chunkCount < 4 {
		chunkCount = 4
	}

	chunks := make([][]int, chunkCount)
	base := n / chunkCount
	rest := n % chunkCount

	index := 0
	for i := 0; i < chunkCount; i++ {
		size := base
		if i < rest {
			size++
		}
		chunks[i] = numbers[index : index+size]
		index += size
	}
	return chunks
}

/* -------------------- CONCURRENT SORT -------------------- */

func sortChunksConcurrently(chunks [][]int) {
	var wg sync.WaitGroup
	wg.Add(len(chunks))

	for i := range chunks {
		go func(i int) {
			defer wg.Done()
			sort.Ints(chunks[i])
		}(i)
	}

	wg.Wait()
}

/* -------------------- MERGE -------------------- */

func mergeSortedChunks(chunks [][]int) []int {
	indexes := make([]int, len(chunks))
	total := 0
	for _, c := range chunks {
		total += len(c)
	}

	result := make([]int, 0, total)

	for len(result) < total {
		minVal := math.MaxInt
		minChunk := -1

		for i, c := range chunks {
			if indexes[i] < len(c) {
				if c[indexes[i]] < minVal {
					minVal = c[indexes[i]]
					minChunk = i
				}
			}
		}

		result = append(result, minVal)
		indexes[minChunk]++
	}

	return result
}

/* -------------------- HELPERS -------------------- */

func printChunks(chunks [][]int) {
	for i, c := range chunks {
		fmt.Printf("Chunk %d: %v\n", i, c)
	}
}
