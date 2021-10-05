package main

import (
	"bufio"
	"bytes"
	"context"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var Cmd = &cobra.Command{
	Use:  "write",
	RunE: run,
}

func init() {
	Cmd.PersistentFlags().StringP("filepath", "f", "", "path to where file will be written")
	Cmd.MarkFlagRequired("filepath")
}

func main() {
	Cmd.Execute()
}

func run(c *cobra.Command, args []string) error {
	path, err := c.Flags().GetString("filepath")
	if err != nil {
		return err
	}

	ctx := context.Background()
	var w *bufio.Writer

	// If the path is a valid google storage url (gs://), write to the bucket.
	if u, err := url.Parse(path); err == nil && u.Scheme == "gs" {
		client, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}
		bucket := client.Bucket(u.Host)
		file := bucket.Object(strings.TrimLeft(u.Path, "/")).NewWriter(ctx)
		defer file.Close()
		w = bufio.NewWriter(file)
		log.Info("writing to google storage", zap.String("host", u.Host), zap.String("path", u.Path))
	} else {
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		w = bufio.NewWriter(file)
		app.Log.Info("writing to disk", zap.String("path", path))
	}

	lines := [][]byte{
		[]byte("The quick brown fox"),
		[]byte("jumped over the lazy dog"),
	}

	if err := write(w, lines); err != nil {
		return err
	}

	return nil
}

func isDir(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return info.IsDir(), err
}

func write(w *bufio.Writer, lines [][]byte) error {
	for _, l := range lines {
		if _, err := w.Write(append(bytes.TrimSpace(l), '\n')); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}
