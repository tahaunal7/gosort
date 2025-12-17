/*
Name: Mehmet Taha Ünal
Student ID: 231AMB077
Assignment: Concurrent Chunk Sorting (gosort)
*/

package main

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* -------------------- CHUNKING -------------------- */

func chunkCount(n int) int {
	c := int(math.Ceil(math.Sqrt(float64(n))))
	if c < 4 {
		return 4
	}
	return c
}

func splitIntoChunks(nums []int) [][]int {
	n := len(nums)
	c := chunkCount(n)

	chunks := make([][]int, c)
	base := n / c
	rest := n % c

	index := 0
	for i := 0; i < c; i++ {
		size := base
		if i < rest {
			size++
		}
		chunks[i] = nums[index : index+size]
		index += size
	}
	return chunks
}

/* -------------------- CONCURRENT SORT -------------------- */

func sortChunks(chunks [][]int) {
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

/* -------------------- MERGE (K-WAY) -------------------- */

func mergeChunks(chunks [][]int) []int {
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

/* -------------------- MODE -r -------------------- */

func modeRandom(n int) error {
	if n < 10 {
		return errors.New("N must be >= 10")
	}

	rand.Seed(time.Now().UnixNano())
	nums := make([]int, n)
	for i := range nums {
		nums[i] = rand.Intn(1000) // range: 0–999
	}

	fmt.Println("Original:", nums)

	chunks := splitIntoChunks(nums)
	fmt.Println("\nChunks before sorting:")
	for _, c := range chunks {
		fmt.Println(c)
	}

	sortChunks(chunks)

	fmt.Println("\nChunks after sorting:")
	for _, c := range chunks {
		fmt.Println(c)
	}

	merged := mergeChunks(chunks)
	fmt.Println("\nFinal merged result:")
	fmt.Println(merged)

	return nil
}

/* -------------------- MODE -i -------------------- */

func readFileInts(path string) ([]int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var nums []int
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		v, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", line)
		}
		nums = append(nums, v)
	}

	if len(nums) < 10 {
		return nil, errors.New("fewer than 10 valid numbers")
	}
	return nums, nil
}

func modeInputFile(path string) error {
	nums, err := readFileInts(path)
	if err != nil {
		return err
	}

	fmt.Println("Original:", nums)

	chunks := splitIntoChunks(nums)
	fmt.Println("\nChunks before sorting:")
	for _, c := range chunks {
		fmt.Println(c)
	}

	sortChunks(chunks)

	fmt.Println("\nChunks after sorting:")
	for _, c := range chunks {
		fmt.Println(c)
	}

	merged := mergeChunks(chunks)
	fmt.Println("\nFinal merged result:")
	fmt.Println(merged)

	return nil
}

/* -------------------- MODE -d -------------------- */

func modeDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return errors.New("invalid directory")
	}

	outDir := fmt.Sprintf("%s_sorted_mehmet_taha_unal_231AMB077", dir)
	err = os.MkdirAll(outDir, 0755)
	if err != nil {
		return err
	}

	files, _ := filepath.Glob(filepath.Join(dir, "*.txt"))

	for _, f := range files {
		nums, err := readFileInts(f)
		if err != nil {
			return err
		}

		chunks := splitIntoChunks(nums)
		sortChunks(chunks)
		merged := mergeChunks(chunks)

		outFile := filepath.Join(outDir, filepath.Base(f))
		file, err := os.Create(outFile)
		if err != nil {
			return err
		}

		w := bufio.NewWriter(file)
		for _, v := range merged {
			fmt.Fprintln(w, v)
		}
		w.Flush()
		file.Close()
	}

	return nil
}

/* -------------------- MAIN -------------------- */

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: gosort -r N | -i file | -d directory")
		return
	}

	switch os.Args[1] {
	case "-r":
		n, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("Invalid N")
			return
		}
		if err := modeRandom(n); err != nil {
			fmt.Println("Error:", err)
		}

	case "-i":
		if err := modeInputFile(os.Args[2]); err != nil {
			fmt.Println("Error:", err)
		}

	case "-d":
		if err := modeDirectory(os.Args[2]); err != nil {
			fmt.Println("Error:", err)
		}

	default:
		fmt.Println("Unknown mode")
	}
}
