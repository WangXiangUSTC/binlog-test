package main

import (
	"compress/flate"
	"flag"

	"github.com/juju/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("binlogTest", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.Mode, "mode", "normal", "mode")
	fs.IntVar(&cfg.Count, "count", 10000, "count")
	fs.StringVar(&cfg.Compress, "compress", "Y", "compress")
	fs.IntVar(&cfg.CompressLevel, "compress-level", flate.BestSpeed, "compress level")
	fs.StringVar(&cfg.Method, "method", "gzip", "method")
	fs.IntVar(&cfg.Size, "size", 1000, "size")
	return cfg
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	Mode          string `json:"mode"`
	Count         int    `json:"count"`
	Compress      string `json:"compress"`
	CompressLevel int    `json:"compress-level"`
	Method        string `json:"method"`
	Size          int    `json:"size"`
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
