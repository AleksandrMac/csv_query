package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	OutputPath      string `json:"outputPath" yaml:"outputPath"`
	ErrorOutputPath string `json:"errorOutputPath" yaml:"errorOutputPath"`
}

func New(conf Config) (*zap.Logger, error) {
	var (
		fInfo *os.File
		fErr  *os.File
		err   error
	)

	if fInfo, err = os.OpenFile(conf.OutputPath, os.O_APPEND|os.O_CREATE, 0755); err != nil {
		return nil, err
	}
	if fErr, err = os.OpenFile(conf.ErrorOutputPath, os.O_APPEND|os.O_CREATE, 0755); err != nil {
		return nil, err
	}

	infoLvl := func(lvl zapcore.Level) bool { return lvl < zapcore.ErrorLevel }
	errLvl := func(lvl zapcore.Level) bool { return lvl >= zapcore.WarnLevel }
	return zap.New(zapcore.NewTee(
		zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(fInfo), zap.LevelEnablerFunc(infoLvl)),
		zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(fErr), zap.LevelEnablerFunc(errLvl)),
	)), nil
}
