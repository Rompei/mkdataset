package main

import (
	"bufio"
	"fmt"
	"github.com/jessevdk/go-flags"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var comma = regexp.MustCompile(`^\.`)

// Options is struct of Options.
type Options struct {
	Output       string `short:"o" long:"output" description:"output dir" default:"output"`
	DataDir      string `short:"d" long:"datadir" description:"Directory of datasets." default:"./"`
	IsMakeTxt    bool   `short:"t" long:"txt" description:"Making summary text file"`
	Label        string `short:"l" long:"label" description:"Label of the dataset"`
	TextFileName string `short:"f" long:"txtfname" description:"The name of text file"`
	Prefix       string `short:"p" long:"prefix" description:"Prefix of the data."`
	Suffix       string `short:"s" long:"suffix" description:"Suffix of the data."`
}

const (
	// B is byte
	B = 1
	// KB is kilo byte
	KB = 1024 * B
	// MB is mega byte
	MB = 1024 * KB
	//GB is giga byte
	GB = 1024 * MB
)

//DiskStatus is status of disk.
type DiskStatus struct {
	All  uint64 `json:"all"`
	Used uint64 `json:"used"`
	Free uint64 `json:"free"`
}

// diskUsage is disk usage of path/disk
func diskUsage(path string) (disk DiskStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return
}

func main() {
	rand.Seed(time.Now().UnixNano())
	opts, err := parseFlags()
	if err != nil {
		panic(err)
	}
	err = walk(opts.DataDir, opts)
	if err != nil {
		err = handleError(opts)
		if err != nil {
			panic(err)
		}
	}
}

func parseFlags() (*Options, error) {
	var opts Options
	_, err := flags.Parse(&opts)
	if err != nil {
		os.Exit(0)
	}
	if opts.IsMakeTxt {
		if opts.Label == "" || opts.TextFileName == "" {
			return nil, fmt.Errorf("Label or text file name is not defined.")
		}
	}
	if !isFileExist(opts.Output) {
		if err := os.MkdirAll(opts.Output, 0777); err != nil {
			return nil, err
		}
	}
	return &opts, nil
}

func isFileExist(fpath string) bool {
	_, err := os.Stat(fpath)
	if pathError, ok := err.(*os.PathError); ok {
		if pathError.Err == syscall.ENOTDIR {
			return false
		}
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func walk(root string, opts *Options) error {
	distStatus := diskUsage("/")
	var amount int64
	var paths []string
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		ok, err := checkFilePath(info, path, opts)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}
		// count a sum of file size.
		amount += info.Size()
		paths = append(paths, path)
		return nil
	})

	// Caluculate predicted capacity after saving data.
	capPredict := distStatus.Free - uint64(amount)
	if float64(distStatus.Free-uint64(amount)) < 3e+10 {
		return fmt.Errorf("Shortage of disk capacity.")
	}
	res, err := waitUserAction(fmt.Sprintf("Disk capacity will be %.2f/%.2f(GB)(file num: %d). Do you continue? [Y/N]", float64(capPredict)/float64(GB), float64(distStatus.All)/float64(GB), len(paths)))
	if err != nil {
		return err
	}
	if res == false {
		handleError(opts)
		return nil
	}

	// Create indices.
	indices := make([]int, len(paths))
	for i := 0; i < len(paths); i++ {
		indices[i] = i
	}
	// Shuffle indices.
	shuffle(indices)

	// Walking dirs.
	var out *os.File
	if opts.IsMakeTxt {
		var err error
		out, err = os.Create(opts.TextFileName)
		if err != nil {
			return err
		}
		defer out.Close()
	}

	for i, sPath := range paths {
		dPath, err := buildPath(indices[i], opts.Output, sPath)
		if err != nil {
			return err
		}
		if opts.IsMakeTxt {
			_, err := out.WriteString(fmt.Sprintf("%s %s\n", dPath, opts.Label))
			if err != nil {
				return err
			}
		}
		err = copyFile(sPath, dPath)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "\r%.1f%%...", float64(i)/float64(len(paths)-1)*100)
	}
	fmt.Println()
	return nil
}

func buildPath(idx int, dir string, sPath string) (string, error) {
	if !filepath.IsAbs(dir) {
		var err error
		dir, err = filepath.Abs(dir)
		if err != nil {
			return "", err
		}
	}
	return dir + "/" + strconv.Itoa(idx) + filepath.Ext(sPath), nil
}

func checkFilePath(info os.FileInfo, path string, opts *Options) (bool, error) {
	var pre *regexp.Regexp
	var suf *regexp.Regexp
	var err error
	if opts.Prefix != "" {
		pre, err = regexp.Compile("^" + opts.Prefix)
		if err != nil {
			return false, err
		}
		if !pre.MatchString(filepath.Base(path)) {
			return false, nil
		}
	}
	if opts.Suffix != "" {
		suf, err = regexp.Compile(opts.Suffix + "$")
		if err != nil {
			return false, err
		}
		if !suf.MatchString(filepath.Base(path)) {
			return false, nil
		}
	}
	splited := strings.Split(path, "/")
	for _, v := range splited {
		if comma.MatchString(v) {

			return false, nil
		}
	}
	return !info.IsDir(), nil
}

func copyFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}
	return nil
}

func shuffle(indices []int) {
	for i := range indices {
		j := rand.Intn(i + 1)
		indices[i], indices[j] = indices[j], indices[i]
	}
}

func handleError(opts *Options) error {
	if err := os.RemoveAll(opts.Output); err != nil {
		return err
	}
	if opts.IsMakeTxt && isFileExist(opts.TextFileName) {
		if err := os.Remove(opts.TextFileName); err != nil {
			return err
		}
	}
	return nil
}

func waitUserAction(msg string) (bool, error) {
	fmt.Printf("%s >", msg)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		t := scanner.Text()
		if t == "Yes" || t == "y" || t == "yes" || t == "Y" || t == "YES" {
			return true, nil
		}
		if t == "No" || t == "n" || t == "no" || t == "N" || t == "NO" {
			return false, nil
		}
		break
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}
