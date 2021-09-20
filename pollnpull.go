package pollnpull

import (
	"context"
	"fmt"
	"log"
	"time"
)

type PollNPull struct {
	source DataSource
	target DataTarget

	pollInterval time.Duration
}

func NewPollNPull(src DataSource, tgt DataTarget, interval time.Duration) *PollNPull {
	return &PollNPull{
		source:       src,
		target:       tgt,
		pollInterval: interval,
	}
}

func (pp *PollNPull) Run(ctx context.Context) error {
	if err := pp.sync(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled")

		case <-time.Tick(pp.pollInterval):
			if err := pp.sync(ctx); err != nil {
				return err
			}
		}
	}
}

func (pp PollNPull) sync(ctx context.Context) error {
	ids, err := pp.target.ListDeveloperIDS(ctx)
	if err != nil {
		return err
	}

	newDevs, err := pp.source.Delta(ctx, ids)
	if err != nil {
		return err
	}

	if err := pp.target.PersistDevelopers(ctx, newDevs); err != nil {
		return err
	}

	log.Printf("%d new developers are persisted to the target", len(newDevs))
	return nil
}