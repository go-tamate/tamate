package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Mitu217/tamate/table/schema"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
)

const (
	APIKeyJSON = `{"installed":{"client_id":"91207976446-6jhqnhlqitbv60fskj0uulq3hf2iil1t.apps.googleusercontent.com","project_id":"nifty-inkwell-197212","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://accounts.google.com/o/oauth2/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"Baxr8JY1P5WD1ggkn1aRptzR","redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]}}`
)

// SpreadSheetsConfig :
type SpreadsheetTableConfig struct {
	SpreadSheetsID string `json:"spreadsheets_id"`
	SheetName      string `json:"sheet_name"`
	Range          string `json:"range"`
}

// SpreadsheetTable :
type SpreadsheetTable struct {
	schema *schema.Schema
	config *SpreadsheetTableConfig
}

// NewSpreadsheet :
func NewSpreadsheet(sc *schema.Schema, conf *SpreadsheetTableConfig) (*SpreadsheetTable, error) {
	ds := &SpreadsheetTable{
		schema: sc,
		config: conf,
	}
	return ds, nil
}

func contains(s []string, e string) int {
	for i, v := range s {
		if e == v {
			return i
		}
	}
	return -1
}

// GetSchema :
func (tbl *SpreadsheetTable) GetSchema() (*schema.Schema, error) {
	return tbl.schema, nil
}

// SetSchema :
func (ds *SpreadsheetTable) SetSchema(sc *schema.Schema) error {
	ds.schema = sc
	return nil
}

// GetRows :
func (tbl *SpreadsheetTable) GetRows() (*Rows, error) {
	srv := getService()

	// Get data
	readRange := tbl.config.SheetName + "!" + tbl.config.Range
	resp, err := srv.Spreadsheets.Values.Get(tbl.config.SpreadSheetsID, readRange).Do()
	if err != nil {
		return nil, err
	}
	if len(resp.Values) == 0 {
		return nil, errors.New("No data found")
	}
	sheetRows := resp.Values

	values := make([][]string, 0)
	for _, row := range sheetRows {
		if len(row) != len(tbl.schema.Columns) {
			return nil, fmt.Errorf("len(row) != len(columns) on %+v", row)
		}
		value := make([]string, 0)
		for i, _ := range tbl.schema.Columns {
			value = append(value, row[i].(string))
		}
		values = append(values, value)
	}
	return &Rows{
		Values: values,
	}, nil
}

func getService() *sheets.Service {
	ctx := context.Background()

	// If modifying these scopes, delete your previously saved credentials
	// at ~/.credentials/sheets.googleapis.com-go-quickstart.json
	config, err := google.ConfigFromJSON([]byte(APIKeyJSON), "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(ctx, config)

	srv, err := sheets.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets Client %v", err)
	}

	return srv
}

// GetClient uses a Context and config to retrieve a Token
// then generate a Client. It returns the generated Client.
func getClient(ctx context.Context, config *oauth2.Config) *http.Client {
	cacheFile, err := tokenCacheFile()
	if err != nil {
		log.Fatalf("Unable to get path to cached credential file. %v", err)
	}
	tok, err := tokenFromFile(cacheFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(cacheFile, tok)
	}
	return config.Client(ctx, tok)
}

// getTokenFromWeb uses config to request a Token.
// It returns the retrieved Token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(oauth2.NoContext, code)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// tokenCacheFile generates credential file path/filename.
// It returns the generated credential path/filename.
func tokenCacheFile() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	tokenCacheDir := filepath.Join(usr.HomeDir, ".credentials")
	os.MkdirAll(tokenCacheDir, 0700)
	return filepath.Join(tokenCacheDir,
		url.QueryEscape("sheets.googleapis.com-go-quickstart.json")), err
}

// tokenFromFile retrieves a Token from a given file path.
// It returns the retrieved Token and any read error encountered.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	t := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(t)
	defer f.Close()
	return t, err
}

// saveToken uses a file path to create a file and store the
// token in it.
func saveToken(file string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", file)
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}
