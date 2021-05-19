package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AleksandrMac/csv_query/pkg/csv"
	"github.com/AleksandrMac/csv_query/pkg/log"
	toml "github.com/pelletier/go-toml"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type Config struct {
	Head    csv.Head      `json:"head" yaml:"head"`
	Sep     string        `json:"sep" yaml:"sep"`
	TimeOut time.Duration `json:"timeOut" yaml:"timeOut"`
	Log     log.Config    `json:"log" yaml:"log"`
	// OutputPaths      []string `json:"outputPaths" yaml:"outputPaths"`
	// ErrorOutputPaths []string `json:"errorOutputPaths" yaml:"errorOutputPaths"`
	// Log     zap.Config    `json:"log" yaml:"log"`
}

var (
	config        Config
	GitCommit     string
	GitHashCommit string
)

type OutMessage struct {
	Err chan error
	Inf chan string
	Row chan string
}

func main() {
	var (
		err    error
		buf    []byte
		logger *zap.Logger
		path   string
	)

	fs := afero.NewOsFs()

	if buf, err = afero.ReadFile(fs, "configs/config.toml"); err != nil {
		panic(fmt.Errorf("configuration error: %w", err).Error())
	}
	if err = toml.Unmarshal(buf, &config); err != nil {
		panic(fmt.Errorf("configuration error: %w", err).Error())
	}

	if logger, err = log.New(config.Log); err != nil {
		panic(fmt.Errorf("logger created error: %w", err).Error())
	}
	defer func() {
		if errLog := logger.Sync(); errLog != nil {
			fmt.Println(errLog)
			return
		}
	}()

	if path, err = os.Getwd(); err != nil {
		logger.Fatal(err.Error())
	}

	fmt.Println("Working directory: ", path)
	fmt.Println("GitCommit: ", GitCommit)
	fmt.Println("GitHashCommit: ", GitHashCommit)

	ctxMain, cancelMain := context.WithCancel(context.Background())
	defer cancelMain()
	outMessage := OutMessage{
		Err: make(chan error),
		Inf: make(chan string),
	}

	go watchSignals(cancelMain, outMessage)
	go func() {
		for {
			select {
			case <-ctxMain.Done():
				return
			default:
				outMessage.Row = make(chan string)
				linesMatcher(ctxMain, &config, &outMessage, config.TimeOut)
			}
		}
	}()
	for {
		select {
		case <-ctxMain.Done():
			return
		case err := <-outMessage.Err:
			logger.Error(err.Error())
		case message := <-outMessage.Inf:
			logger.Info(message)
		}
	}
}

func watchSignals(cancel context.CancelFunc, outMessage OutMessage) {
	osSignalChan := make(chan os.Signal, 1)

	signal.Notify(osSignalChan,
		syscall.SIGINT)
	sig := <-osSignalChan
	outMessage.Err <- fmt.Errorf("got signal %q", sig.String())

	// если сигнал получен, отменяем контекст работы
	cancel()
}

func fileReader(ctx context.Context, path string, outMessage *OutMessage) {
	defer close(outMessage.Row)
	file, err := os.Open(path)
	if err != nil {
		outMessage.Err <- fmt.Errorf("file open error: %w", err)
		return
	}
	defer file.Close()

	reader := bufio.NewScanner(file)
	for reader.Scan() {
		select {
		case <-ctx.Done():
			outMessage.Err <- ctx.Err()
			return
		default:
			outMessage.Row <- reader.Text()
		}
	}
}

func linesMatcher(ctxParent context.Context, config *Config, outMessage *OutMessage, timeOut time.Duration) {
	sc := bufio.NewScanner(os.Stdin)
	var answer string = "y"

	if config.Head.Fields == nil {
		fmt.Printf(`
В config.toml не найдена информация о названиях полей.
Использовать первую строку в файле %s для инициализации полей?
	
Нажмите Y(да)/N(нет, завершить)`, config.Head.Path)
		if sc.Scan() {
			answer = sc.Text()
		}
	}
	fmt.Print("csv_query>> ")

	var query string = "continent='Asia' and date='2020-04-14'"
	if sc.Scan() {
		query = sc.Text()
		outMessage.Inf <- query
	}

	// nolint:govet
	ctx, _ := context.WithTimeout(ctxParent, timeOut*time.Second)
	go fileReader(ctx, config.Head.Path, outMessage)

	switch answer {
	case "Y", "y":
		var str string = <-outMessage.Row
		config.Head.Fields = csv.GetFields(str, config.Sep)
	default:
		return
	}

	var wgInside sync.WaitGroup
	for val := range outMessage.Row {
		select {
		case <-ctx.Done():
			outMessage.Err <- ctx.Err()
			return
		default:
			wgInside.Add(1)
			go func(value string) {
				defer wgInside.Done()
				if len(value) > 0 {
					row := config.Head.NewRow()
					row.Values = strings.Split(value, config.Sep)
					if row.IsMatch(query) {
						fmt.Println(row.Values)
					}
				}
			}(val)
		}
	}
	wgInside.Wait()
}

// export GIT_COMMIT=$(git rev-list -1 HEAD) && \ go build -ldflags "-X main.GitCommit=$GIT_COMMIT"
// continent='Asia' and date>'2020-04-14'
