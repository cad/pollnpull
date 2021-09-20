package googlesheets

import (
	"context"
	"fmt"
	"github.com/cad/pollnpull"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"log"
	"strings"
)

type DataSource struct {
	sheetID string
	sheetsSvc *sheets.Service

	ParseFn func(s []interface{}) (*pollnpull.Developer, error)
}

func NewDataSource(credsJSON []byte, sheetID string) (*DataSource, error) {
	ctx := context.Background()

	conf, err := google.JWTConfigFromJSON(credsJSON, sheets.SpreadsheetsScope)
	if err != nil {
		return nil, fmt.Errorf("gcloud create config from credentiasl json failed: %v", err)
	}

	svc, err := sheets.NewService(ctx, option.WithHTTPClient(conf.Client(ctx)))
	if err != nil {
		return nil, fmt.Errorf("can not initialize sheets service: %v", err)
	}

	return &DataSource{
		sheetID:   sheetID,
		sheetsSvc: svc,
		ParseFn:   defaultParseFunc,
	}, nil

}

func (ds *DataSource) Delta(ctx context.Context, existingIDColl []string) ([]*pollnpull.Developer, error) {
	rsp, err := ds.sheetsSvc.Spreadsheets.Values.Get(ds.sheetID, "Sheet1!R2C1:C4").Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("can not retrieve spreadsheet values for '%s': %v", ds.sheetID, err)
	}

	var devColl []*pollnpull.Developer
	for _, v := range rsp.Values {
		dev, err := ds.ParseFn(v)
		if err != nil {
			log.Printf("can not parse '%v' (skipping): %v", v, err)
			continue
		}

		var found bool
		for _, existingID := range existingIDColl {
			if dev.ID == existingID {
				found = true
				break
			}
		}

		// Include parsed dev in the delta IFF it does not already exist.
		if !found {
			devColl = append(devColl, dev)
		}
	}

	return devColl, nil
}


func defaultParseFunc(s []interface{}) (*pollnpull.Developer, error) {
	var dev pollnpull.Developer

	if len(s) > 0 {
		if v, ok := s[0].(string); ok && len(v) > 0{
			dev.ID = strings.TrimSpace(v)
		}
	}

	if len(s) > 1 {
		if v, ok := s[1].(string); ok && len(v) > 0 {
			dev.FullName = strings.TrimSpace(v)
		}
	}

	if len(s) > 2 {
		if v, ok := s[2].(string); ok && len(v) > 0 {
			dev.Organization = strings.TrimSpace(v)
		}
	}

	if len(s) > 3 {
		if v, ok := s[3].(string); ok && len(v) > 0 {
			dev.ContactHandle = strings.TrimSpace(v)
		}
	}

	// TODO: Refactor this into a dynamic column name column index
	// mapper in order to get rid of the code duplication.

	if len(dev.ID) == 0 || len(dev.FullName) == 0 {
		return nil, fmt.Errorf("can not parse dev '%v' has invalid or missing data", dev)
	}

	return &dev, nil
}