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
	logConf.OutputPaths = config.OutputPaths
	logConf.ErrorOutputPaths = config.ErrorOutputPaths
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
	errors := make(chan error)
	go watchSignals(cancelMain, errors)
	go func() {
		var wg sync.WaitGroup
		for {
			ctx, cancel := context.WithTimeout(ctxMain, config.TimeOut*time.Second)
			lines := make(chan string)
			//nolint:gomnd
			wg.Add(2)
			go fileReader(ctx, cancel, config.Head.Path, lines, errors, &wg)
			go linesMatcher(ctx, &config, lines, errors, &wg)
			wg.Wait()
		}
	}()
	for {
		select {
		case <-ctxMain.Done():
			return
		case err := <-errors:
			logger.Error(err.Error())
		}
	}
}

func watchSignals(cancel context.CancelFunc, errCh chan error) {
	osSignalChan := make(chan os.Signal, 1)

	signal.Notify(osSignalChan,
		syscall.SIGINT)
	sig := <-osSignalChan
	errCh <- fmt.Errorf("got signal %q", sig.String())

	// если сигнал получен, отменяем контекст работы
	cancel()
}

func fileReader(ctx context.Context, cancel context.CancelFunc, path string, lineCh chan string, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(lineCh)
	file, err := os.Open(path)
	if err != nil {
		errCh <- fmt.Errorf("file open error: %w", err)
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
			lineCh <- reader.Text()
		}
	}
}

func linesMatcher(ctx context.Context, config *Config, lineCh chan string, errCh chan error, wg *sync.WaitGroup) {
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
			var str string = <-lineCh
			config.Head.Fields = csv.GetFields(str, config.Sep)
		default:
			return
		}
	}
	fmt.Print("csv_query>> ")

	var query string
	if sc.Scan() {
		query = sc.Text()
		config.Head.Log.Info(query)
	}
	var wgInside sync.WaitGroup

	for val := range lineCh {
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
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
// continent='Asia' and date='2020-04-14'
