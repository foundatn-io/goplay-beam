package spannerio

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
)

type Client struct {
	*spanner.Client
	Config Config
}

type Config struct {
	Project  string
	Instance string
	Database string
}

func (c *Config) AsConnectionString() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", c.Project, c.Instance, c.Database)
}

func New(ctx context.Context, cfg *Config) (*Client, error) {
	client, err := spanner.NewClient(ctx, cfg.AsConnectionString())
	if err != nil {
		return nil, err
	}

	return &Client{Client: client}, nil
}
