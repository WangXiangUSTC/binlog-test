package main

import (
	"flag"

	//"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("binlogTest", flag.ContinueOnError)
	fs := cfg.FlagSet

	//fs.StringVar(&cfg.ConfigFile, "config", "", "Config file")
	//fs.IntVar(&cfg.WorkerCount, "c", 1, "parallel worker count")
	//fs.IntVar(&cfg.JobCount, "n", 1, "total job count")
	//fs.IntVar(&cfg.Batch, "b", 1, "insert batch commit count")
	//fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	//fs.StringVar(&cfg.JobType, "jobType", "", "jobType: createDataTube, loadData, check, clear")
	//fs.StringVar(&cfg.EtherUrl, "etherUrl", "", "ether url")
	fs.StringVar(&cfg.Mode, "mode", "normal", "mode")
	fs.IntVar(&cfg.Count, "count", 10000, "count")
	fs.StringVar(&cfg.Compress, "compress", "Y", "compress")
	fs.StringVar(&cfg.Method, "method", "gzip", "method")
	fs.IntVar(&cfg.Size, "size", 1000, "size")
	return cfg
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	Mode     string `json:"mode"`
	Count    int    `json:"count"`
	Compress string `json:"compress"`
	Method   string `json:"method"`
	Size     int    `json:"size"`
	/*
		LogLevel string `toml:"log-level" json:"log-level"`

		WorkerCount int `toml:"worker-count" json:"worker-count"`

		JobCount int `toml:"job-count" json:"job-count"`

		Batch int `toml:"batch" json:"batch"`

		SourceDBCfg util.DBConfig `toml:"source-db" json:"source-db"`

		TargetDBCfg util.DBConfig `toml:"target-db" json:"target-db"`

		ConfigFile string

		JobType string `toml:"job-type" json:"job-type"`

		EtherUrl string `toml:"ether-url" json:"ether-url"`
	*/
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}
