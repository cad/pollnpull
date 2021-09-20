package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/cad/pollnpull"
	"github.com/cad/pollnpull/pkg/source/googlesheets"
	"github.com/cad/pollnpull/pkg/target/sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	sheetID = flag.String("sheet-id", "", "Spreadsheet ID")
	gcloudCredentialsJSON = flag.String("gcloud-creds", "", "Google Cloud Credentials JSON file")
	dbFilePath = flag.String("sqlite3-db", "db.sqlite3", "sqlite3 db file")
)

func main() {
	flag.Parse()
	if len(*sheetID) == 0 {
		log.Fatalf("-sheet-id is required and must be valid google sheets spreadsheet id")
	}

	ctx, cancel := context.WithCancel(context.Background())
	registerSigInt(ctx, cancel)

	clientConfigJSON, err := ioutil.ReadFile(*gcloudCredentialsJSON)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	src, err := googlesheets.NewDataSource(clientConfigJSON, *sheetID)
	if err != nil {
		log.Fatalf("can not initialize data source: %v", err)
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s", *dbFilePath))
	if err != nil {
		log.Fatal(err)
	}

	// Migrate
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS developers (
    id VARCHAR(64) NOT NULL PRIMARY KEY,
    full_name VARCHAR(256) NOT NULL,
    organization VARCHAR(512) NULL,
    contact_handle VARCHAR(512) NULL    
    )
`); err != nil {
		log.Fatal(err)
	}

	tgt, err := sqlite3.NewDataTarget(db)
	if err != nil {
		log.Fatalf("can not initialize data target: %v", err)
	}

	pollnpull.NewPollNPull(src, tgt, time.Second * 30).Run(ctx)
}

func registerSigInt(ctx context.Context, cancel func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

}