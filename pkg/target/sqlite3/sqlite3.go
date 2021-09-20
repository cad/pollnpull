package sqlite3

import (
	"context"
	"database/sql"
	"github.com/cad/pollnpull"
	"log"
)

type DataTarget struct {
	db *sql.DB
}

func NewDataTarget(db *sql.DB) (*DataTarget, error) {
	return &DataTarget{
		db: db,
	}, nil
}

func (dt *DataTarget) ListDeveloperIDS(ctx context.Context) ([]string, error) {
	rows, err := dt.db.Query(`SELECT id FROM developers`)
	if err != nil {
		return nil, err
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (dt *DataTarget) PersistDevelopers(ctx context.Context, devColl []*pollnpull.Developer) error {
	tx, err := dt.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	select {
	case <- ctx.Done():
		return tx.Rollback()
	default:
		for _, dev := range devColl {
			id := dev.ID
			fullName := dev.FullName
			org := dev.Organization
			if len(org) == 0 {
				org = "NULL"
			}
			contactHandle := dev.ContactHandle
			if len(contactHandle) == 0 {
				contactHandle = "NULL"
			}
			if _, err := tx.Exec(`INSERT INTO developers (id, full_name, organization, contact_handle) VALUES (?, ?, ?, ?)`, id, fullName, org, contactHandle); err != nil {
				log.Println(err)
				continue
			}
		}
	}

	return tx.Commit()
}