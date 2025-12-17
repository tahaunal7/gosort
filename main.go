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

/* ---------- CHUNK CALCULATION ---------- */

func computeChunkTotal(count int) int {
	k := int(math.Ceil(math.Sqrt(float64(count))))
	if k < 4 {
		return 4
	}
	return k
}

func distributeIntoParts(data []int) [][]int {
	total := len(data)
	partCount := computeChunkTotal(total)

	parts := make([][]int, partCount)
	minSize := total / partCount
	remainder := total % partCount

	cursor := 0
	for i := 0; i < partCount; i++ {
		currentSize := minSize
		if i < remainder {
			currentSize++
		}
		parts[i] = data[cursor : cursor+currentSize]
		cursor += currentSize
	}
	return parts
}

/* ---------- CONCURRENT SORT ---------- */

func parallelSort(parts [][]int) {
	var wg sync.WaitGroup
	wg.Add(len(parts))

	for idx := range parts {
		go func(i int) {
			defer wg.Done()
			sort.Ints(parts[i])
		}(idx)
	}

	wg.Wait()
}

/* ---------- MERGING ---------- */

func mergeSortedParts(parts [][]int) []int {
	pos := make([]int, len(parts))
	finalSize := 0

	for _, p := range parts {
		finalSize += len(p)
	}

	merged := make([]int, 0, finalSize)

	for len(merged) < finalSize {
		smallest := math.MaxInt
		source := -1

		for i := range parts {
			if pos[i] < len(parts[i]) {
				if parts[i][pos[i]] < smallest {
					smallest = parts[i][pos[i]]
					source = i
				}
			}
		}

		merged = append(merged, smallest)
		pos[source]++
	}

	return merged
}

/* ---------- RANDOM MODE ---------- */

func runRandomMode(n int) error {
	if n < 10 {
		return errors.New("number of elements must be >= 10")
	}

	rand.Seed(time.Now().UnixNano())
	values := make([]int, n)

	for i := 0; i < n; i++ {
		values[i] = rand.Intn(1000)
	}

	fmt.Println(values)

	segments := distributeIntoParts(values)

	for _, s := range segments {
		fmt.Println(s)
	}

	parallelSort(segments)

	for _, s := range segments {
		fmt.Println(s)
	}

	result := mergeSortedParts(segments)
	fmt.Println(result)

	return nil
}

/* ---------- FILE MODE ---------- */

func loadNumbersFromFile(path string) ([]int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var numbers []int
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		val, err := strconv.Atoi(text)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", text)
		}
		numbers = append(numbers, val)
	}

	if len(numbers) < 10 {
		return nil, errors.New("not enough numbers in file")
	}

	return numbers, nil
}

func runFileMode(path string) error {
	numbers, err := loadNumbersFromFile(path)
	if err != nil {
		return err
	}

	fmt.Println(numbers)

	segments := distributeIntoParts(numbers)
	parallelSort(segments)

	result := mergeSortedParts(segments)
	fmt.Println(result)

	return nil
}

/* ---------- DIRECTORY MODE ---------- */

func runDirectoryMode(dir string) error {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return errors.New("directory not found")
	}

	targetDir := fmt.Sprintf("%s_sorted_mehmet_taha_unal_231AMB077", dir)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return err
	}

	files, _ := filepath.Glob(filepath.Join(dir, "*.txt"))

	for _, f := range files {
		data, err := loadNumbersFromFile(f)
		if err != nil {
			return err
		}

		segments := distributeIntoParts(data)
		parallelSort(segments)
		sorted := mergeSortedParts(segments)

		outPath := filepath.Join(targetDir, filepath.Base(f))
		outFile, err := os.Create(outPath)
		if err != nil {
			return err
		}

		writer := bufio.NewWriter(outFile)
		for _, v := range sorted {
			fmt.Fprintln(writer, v)
		}
		writer.Flush()
		outFile.Close()
	}

	return nil
}

/* ---------- MAIN ---------- */

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: gosort -r N | -i file | -d directory")
		return
	}

	switch os.Args[1] {
	case "-r":
		n, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("Invalid number")
			return
		}
		if err := runRandomMode(n); err != nil {
			fmt.Println("Error:", err)
		}

	case "-i":
		if err := runFileMode(os.Args[2]); err != nil {
			fmt.Println("Error:", err)
		}

	case "-d":
		if err := runDirectoryMode(os.Args[2]); err != nil {
			fmt.Println("Error:", err)
		}

	default:
		fmt.Println("Unknown option")
	}
}
