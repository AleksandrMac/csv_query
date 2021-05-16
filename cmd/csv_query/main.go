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
	toml "github.com/pelletier/go-toml"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type Config struct {
	Head             csv.Head      `json:"head" yaml:"head"`
	Sep              string        `json:"sep" yaml:"sep"`
	TimeOut          time.Duration `json:"timeOut" yaml:"timeOut"`
	OutputPaths      []string      `json:"outputPaths" yaml:"outputPaths"`
	ErrorOutputPaths []string      `json:"errorOutputPaths" yaml:"errorOutputPaths"`
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
	fs := afero.NewOsFs()
	buf, err := afero.ReadFile(fs, "configs/config.toml")
	if err != nil {
		panic(fmt.Errorf("configuration error: %w", err).Error())
	}
	err = toml.Unmarshal(buf, &config)
	if err != nil {
		panic(fmt.Errorf("configuration error: %w", err).Error())
	}
	logConf := zap.NewProductionConfig()

	if err = pathMatch(&config, fs); err != nil {
		panic(err)
	}

	if len(config.OutputPaths) > 0 {
		logConf.OutputPaths = config.OutputPaths
	}
	if len(config.ErrorOutputPaths) > 0 {
		logConf.ErrorOutputPaths = config.ErrorOutputPaths
	}
	logger, err := logConf.Build()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err1 := logger.Sync(); err1 != nil {
			fmt.Println(err)
			return
		}
	}()

	path, err := os.Getwd()
	if err != nil {
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
		var wg sync.WaitGroup
		for {
			ctx, cancel := context.WithTimeout(ctxMain, config.TimeOut*time.Second)
			//nolint:gomnd
			wg.Add(2)
			go fileReader(ctx, cancel, config.Head.Path, &outMessage, &wg)
			go linesMatcher(ctx, &config, &outMessage, &wg)
			wg.Wait()
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

func fileReader(ctx context.Context, cancel context.CancelFunc, path string, outMessage *OutMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	outMessage.Row = make(chan string)
	defer close(outMessage.Row)
	file, err := os.Open(path)
	if err != nil {
		outMessage.Err <- fmt.Errorf("file open error: %w", err)
		cancel()
		return
	}
	defer file.Close()

	reader := bufio.NewScanner(file)
	for reader.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			outMessage.Row <- reader.Text()
		}
	}
}

func linesMatcher(ctx context.Context, config *Config, outMessage *OutMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	sc := bufio.NewScanner(os.Stdin)

	if config.Head.Fields == nil {
		fmt.Printf(`
В config.toml не найдена информация о названиях полей.
Использовать первую строку в файле %s для инициализации полей?
	
Нажмите Y(да)/N(нет, завершить)`, config.Head.Path)
		var answer string = "y"
		if sc.Scan() {
			answer = sc.Text()
		}

		switch answer {
		case "Y", "y":
			var str string = <-outMessage.Row
			config.Head.Fields = csv.GetFields(str, config.Sep)
			fmt.Println(str)
		default:
			return
		}
	}
	fmt.Print("csv_query>> ")

	var query string = "continent='Asia' and date='2020-04-14'"
	if sc.Scan() {
		query = sc.Text()
		outMessage.Inf <- query
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

func pathMatch(config *Config, fs afero.Fs) error {
	for _, path := range func() []string {
		paths := make([]string, 0, len(config.OutputPaths)+len(config.ErrorOutputPaths))
		paths = append(paths, config.OutputPaths...)
		return append(paths, config.ErrorOutputPaths...)
	}() {
		_, err := fs.Stat(path)
		if err != nil {
			var file afero.File
			file, err = fs.Create(path)
			if err != nil {
				return fmt.Errorf("%v: %w", err.Error(), file.Close())
			}
		}
	}
	return nil
}

// export GIT_COMMIT=$(git rev-list -1 HEAD) && \ go build -ldflags "-X main.GitCommit=$GIT_COMMIT"
// continent='Asia' and date='2020-04-14'
