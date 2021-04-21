package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var urls []string = []string{
	"www.baidu.com",
	"www.qq.com",
	"www.sina.com",
	"www.163.com",
	"www.taobao.com",
	"www.iqiyi.com",
	"www.bilibili.com",
	"www.google.com",
	"www.bing.com",
}

const visit_log = "logs.txt"
const visit_cnt int = 100 * 1000

func generate_log(f string){
	fileHandle, err := os.OpenFile(f, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("open file error :", err)
		return
	}
	buf := bufio.NewWriter(fileHandle)
	//var l int = len(urls)
	for i := 0;i < visit_cnt;i++{
		//var content string = urls[rand.Int()%l]
		var rand_str string
		//rand.Seed(time.Now().Unix())
		for i := 0;i < 4;i++{
			rand_str += string('a' + rand.Int63n(time.Now().Unix()) % 26)
		}
		var content string = "www." + rand_str + ".com"
		// NewWriter 默认缓冲区大小是 4096
		// 需要使用自定义缓冲区的writer 使用 NewWriterSize()方法
		// 字符串写入
		buf.WriteString(content + "\n")
	}
	// 将缓冲中的数据写入
	err = buf.Flush()
	if err != nil {
		log.Println("flush error :", err)
	}
	defer fileHandle.Close()
}

func write_to_singlefile(mychan chan string,filename string,wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	f, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil{
		return
	}

	for true{
		content,ok := <- mychan
		if ok{
			f.WriteString(content + "\n")
		}else{
			break
		}
	}
	f.Close()
	fmt.Println(filename + "write over.")
}

func divide_log(f string){
	fileHandle, err := os.OpenFile(f, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("open file error :", err)
		return
	}

	var ch_group [26]chan string
	var file_isopen [26]bool
	var wg = sync.WaitGroup{}
	scanner := bufio.NewScanner(fileHandle)
	for scanner.Scan() {
		line := scanner.Text()
		// 查找行首以 H 开头，以空格结尾的字符串
		reg := regexp.MustCompile(`^www.(.*).com`)
		site := reg.FindAllStringSubmatch(line,-1)
		if len(site) > 0{
			first_char := site[0][1][0] //simple hash
			//确保每个小文件的尺寸小于内存,如果超过内存，则继续拆分
			file_index := int(first_char - 'a')
			if !file_isopen[file_index]{
				ch_group[file_index] = make(chan string)
				go write_to_singlefile(ch_group[file_index],string(first_char),&wg)
				file_isopen[file_index] = true
			}
			ch_group[file_index] <- line
		}
	}
	for i := 0;i < 26;i++{
		if ch_group[i] != nil{
			close(ch_group[i])
		}
	}
	wg.Wait()
	fmt.Println("all write over.")
}



func sort_singlelog(){
	//var ch_group [26]chan string
	//var file_isopen [26]bool
	var wg = sync.WaitGroup{}
	for i := 0;i < 26;i++ {
		filename := string('a' + i)
		wg.Add(1)
		//go cac_count(filename, &wg)
		go func (f string,wg *sync.WaitGroup){
			defer wg.Done()
			fileHandle, err := os.OpenFile(f, os.O_RDONLY, 0666)
			if err != nil {
				log.Println("open file error :", err)
				return
			}
			var url_cnt map[string]int = make(map[string]int)
			scanner := bufio.NewScanner(fileHandle)
			for scanner.Scan() {
				line := scanner.Text()
				if _,ok := url_cnt[line];ok{
					url_cnt[line]++
				}else{
					url_cnt[line] = 1
				}
			}
			fileHandle.Close()
			var cnt_url map[int][]string = make(map[int][]string)
			var keys map[int]bool = make(map[int]bool)
			for content,cnt := range url_cnt{
				if _,ok := cnt_url[cnt];!ok{
					cnt_url[cnt] = []string{content}
				}else{
					cnt_url[cnt] = append(cnt_url[cnt],content)
				}
				keys[cnt] = true
			}
			var sort_cnt []int
			for k, _ := range keys{
				sort_cnt = append(sort_cnt,k)
			}
			sort.Ints(sort_cnt)
			var l int = len(sort_cnt)
			output_filename := f + "_count"
			output_file, _ := os.OpenFile(output_filename, os.O_TRUNC|os.O_CREATE, 0666)
			defer output_file.Close()
			for i := l - 1;i >= 0;i--{
				for _, content := range cnt_url[sort_cnt[i]]{
					c :=  strconv.Itoa(sort_cnt[i]) + " " + content + "\n"
					output_file.WriteString(c)
				}
			}
		}(filename,&wg)
	}
	wg.Wait()
	fmt.Println("All count finished!")
}

type Cnt_url_pair struct{
	cnt int
	url string
}
type MaxcntUrlHeap []Cnt_url_pair

func (h MaxcntUrlHeap) Len() int {
	return len(h)
}

func (h MaxcntUrlHeap) Less(i, j int) bool {
	// 由于是最大堆，所以使用大于号
	return h[i].cnt < h[j].cnt
}

func (h *MaxcntUrlHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *MaxcntUrlHeap) Push(x interface{}) {
	*h = append(*h, x.(Cnt_url_pair))
}

// Pop 弹出最后一个元素
func (h *MaxcntUrlHeap) Pop() interface{}{
	res := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return res
}

func topK(k int){
	//var q []Cnt_url_pair
	h := make(MaxcntUrlHeap,0)
	heap.Init(&h)

	for i := 0;i < 26;i++ {
		filename := string('a' + i) + "_count"
		fileHandle, err := os.OpenFile(filename, os.O_RDONLY, 0666)
		if err != nil {
			log.Println("open file error :", err)
			return
		}
		defer fileHandle.Close()
		scanner := bufio.NewScanner(fileHandle)
		for scanner.Scan() {
			line := scanner.Text()
			content := strings.Split(line," ")
			cnt,_ := strconv.Atoi(content[0])
			if len(h) < k{
				var obj Cnt_url_pair
				obj.cnt = cnt
				obj.url = content[1]
				heap.Push(&h,obj)
			}else{
				if cnt <= h[0].cnt {
					break
				}
				heap.Pop(&h)
				var obj Cnt_url_pair
				obj.cnt = cnt
				obj.url = content[1]
				heap.Push(&h,obj)
			}
		}
	}

	for h.Len() > 0 {
		var obj Cnt_url_pair =  heap.Pop(&h).(Cnt_url_pair)
		fmt.Println("cnt = " + strconv.Itoa(obj.cnt) +" url = " + obj.url)
	}
}

func main(){
	//1 generate 1 million url visits
	generate_log(visit_log);
	//map (divide and cut)
	//2 If kinds of url are very large,maybe exceed memory limit,we should divide url to a single file by hash(url)
	//逐行读取文件，hash值相同的放入同一个文件中 hash(url) % 26
	divide_log(visit_log)
	//reduce (count each single file)
	sort_singlelog()
	//Get top K by small top heap
	var k int = 10
	topK(k)
}
