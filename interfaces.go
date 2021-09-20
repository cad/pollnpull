package pollnpull

import "context"

type DataSource interface {
	Delta(ctx context.Context, existingIDColl []string) ([]*Developer, error)
}

type DataTarget interface {
	ListDeveloperIDS(ctx context.Context) ([]string, error)
	PersistDevelopers(ctx context.Context, devColl []*Developer) error
}