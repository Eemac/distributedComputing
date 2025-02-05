package main

	
import (
	"bufio"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "regexp"
    "sort"
    "sync"
    "time"
    "runtime"
)

// TODO: Your single-threaded implementation
func single_threaded(files []string) {
	wordMap := make(map[string]int)
	for i := 0; i < len(files); i++ {
		//fmt.Printf("Reading File: %s...\n", files[i])
		

		//Open new file
		file, _ := os.Open(files[i])
		defer file.Close()
		
		//Scan in line by line...
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.ToLower(scanner.Text())
			line = regexp.MustCompile(`[^a-zA-Z0-9 ]+`).ReplaceAllString(line, " ")
			words := strings.Fields(line)

		    for _, word := range words {
		    	wordMap[word] += 1
		    }  
		}
	}

	//Alphabetize list
	keys := make([]string, 0, len(wordMap))
	for key := range wordMap {
        keys = append(keys, key)
    }
	sort.Strings(keys)

	f, err := os.Create("./output/single.txt")
	check(err)


	var totalBytes int = 0

	for _, key := range keys {
		n_bytes, _ := f.WriteString(fmt.Sprintf("%s %d\n", string(key), wordMap[key]))
		totalBytes += n_bytes
	}

	//fmt.Printf("Sorted %d words\n", len(keys))

	f.Sync()

    //fmt.Printf("wrote %d bytes to file \n", totalBytes)
    defer f.Close()
}

// SafeCounter is safe to use concurrently.
type SharedData struct {
	mu sync.Mutex
	wordMap  map[string]int
	wg sync.WaitGroup
}


// TODO: Your multi-threaded implementation
func multi_threaded(files []string) {
	s := SharedData{wordMap: make(map[string]int)}
	
	s.wg.Add(len(files))
	
	
	for i := 0; i < len(files); i++ {
		go s.doWordCount(files[i])
	}

	s.wg.Wait()

	//Alphabetize list
	keys := make([]string, 0, len(s.wordMap))
	for key := range s.wordMap {
        keys = append(keys, key)
    }
	sort.Strings(keys)

	f, err := os.Create("./output/multi.txt")
	check(err)


	var totalBytes int = 0

	for _, key := range keys {
		n_bytes, _ := f.WriteString(fmt.Sprintf("%s %d\n", string(key), s.wordMap[key]))
		totalBytes += n_bytes
	}

	// fmt.Printf("Sorted %d words\n", len(keys))

	f.Sync()

    // fmt.Printf("wrote %d bytes to file \n", totalBytes)
    defer f.Close()
}


func main() {
	var (
        files []string
        err   error
	)

	runtime.GOMAXPROCS(6)
	fmt.Println(runtime.NumCPU())

	
	//Get command-line argument for folder directory
	cliArg := os.Args[1]
	files, err = FilePathWalkDir(cliArg)
	if err != nil {
		panic(err)
	}

	//If directory exists...
	if _, err := os.Stat("./output"); err == nil {
		//Remove previous files
		err = os.RemoveAll("./output")
		check(err)
	}

    // Create the directory with the specified path if there isn't one
    err = os.Mkdir("./output", 0755) // 0755 sets permissions for the directory
    start := time.Now()
	single_threaded(files);

	elapsed := time.Since(start)
	fmt.Printf("single took %s\n", elapsed)


	start = time.Now()
	multi_threaded(files);
	elapsed = time.Since(start)
	fmt.Printf("multi took %s\n", elapsed)

	// TODO: add argument processing and run both single-threaded and multi-threaded functions
}

func (s *SharedData) doWordCount(filePath string) {
	defer s.wg.Done()
	wc := make(map[string]int)
	// fmt.Printf("Reading File: %s...\n", filePath)
	

	//Open new file
	file, _ := os.Open(filePath)
	defer file.Close()
	
	//Scan in line by line...
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.ToLower(scanner.Text())
		line = regexp.MustCompile(`[^a-zA-Z0-9 ]+`).ReplaceAllString(line, " ")
		words := strings.Fields(line)

	    for _, word := range words {
	    	wc[word] += 1
	    }  
	}

	//Transfer list

	s.mu.Lock()
	for w, _ := range wc {
		s.wordMap[w] += wc[w]
	}
	s.mu.Unlock()
}


func FilePathWalkDir(root string) ([]string, error) {
    var files []string
    err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
        if !info.IsDir() {
            files = append(files, path)
        }
        return nil
    })
    return files, err
}

func check(e error) {
	if e != nil {
		fmt.Println(e)
	}
}