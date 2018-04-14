package datasource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/Mitu217/tamate/schema"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	sheets "google.golang.org/api/sheets/v4"
	"runtime"
)

// SpreadSheetsConfig :
type SpreadSheetsDatasourceConfig struct {
	Type           string `json:"type"`
	SpreadSheetsID string `json:"spreadsheets_id"`
	SheetName      string `json:"sheet_name"`
	Range          string `json:"range"`
}

// SpreadSheetsDataSource :
type SpreadSheetsDataSource struct {
	Config *SpreadSheetsDatasourceConfig
	Schema *schema.Schema
}

// NewSpreadSheetsDataSource :
func NewSpreadSheetsDataSource(config *SpreadSheetsDatasourceConfig) (*SpreadSheetsDataSource, error) {
	ds := &SpreadSheetsDataSource{
		Config: config,
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
func (ds *SpreadSheetsDataSource) GetSchema() (*schema.Schema, error) {
	if ds.Schema != nil {
		return ds.Schema, nil
	}

	srv := getService()

	// Get data
	readRange := ds.Config.SheetName + "!" + ds.Config.Range
	resp, err := srv.Spreadsheets.Values.Get(ds.Config.SpreadSheetsID, readRange).Do()
	if err != nil {
		return nil, err
	}
	if len(resp.Values) == 0 {
		return nil, errors.New("No data found")
	}
	sheetRows := resp.Values

	// Get Schema
	return ds.getSchema(sheetRows)
}

func (ds *SpreadSheetsDataSource) getSchema(rows [][]interface{}) (*schema.Schema, error) {
	if ds.Schema != nil {
		return ds.Schema, nil
	}

	sc := &schema.Schema{} //FIXME: Schemaを統合した後に修正
	for _, row := range rows {
		tagField := row[0]
		if tagField == "COLUMN" {
			var columns []schema.Column
			for _, col := range row[1:] {
				columns = append(columns, schema.Column{
					Name: col.(string),
					Type: "text",
				})
			}
			sc.Columns = columns
			break
		}
	}
	if len(sc.Columns) == 0 {
		return nil, errors.New("No columns in SpreadSheets")
	}

	return sc, nil
}

// SetSchema :
func (ds *SpreadSheetsDataSource) SetSchema(sc *schema.Schema) error {
	ds.Schema = sc
	return nil
}

// GetRows :
func (ds *SpreadSheetsDataSource) GetRows() (*Rows, error) {
	srv := getService()

	// Get data
	readRange := ds.Config.SheetName + "!" + ds.Config.Range
	resp, err := srv.Spreadsheets.Values.Get(ds.Config.SpreadSheetsID, readRange).Do()
	if err != nil {
		return nil, err
	}
	if len(resp.Values) == 0 {
		return nil, errors.New("No data found")
	}
	sheetRows := resp.Values

	// Get Schema
	sc, err := ds.getSchema(sheetRows)
	if err != nil {
		return nil, err
	}

	// Get Columns
	var columnNames []string
	for _, row := range sheetRows {
		tagField := row[0]
		if tagField == "COLUMN" {
			for _, field := range row[1:] {
				columnNames = append(columnNames, field.(string))
			}
		}
	}
	if len(columnNames) == 0 {
		return nil, errors.New("No columns in SpreadSheets. SheetID: " + ds.Config.SpreadSheetsID)
	}

	// Get data
	values := make([][]string, 0)
	for _, row := range sheetRows {
		tagField := row[0]
		switch tagField {
		case "COLUMN":
			continue
		case "":
			// Get Data
			value := make([]string, 0)
			for _, column := range sc.Columns {
				index := contains(columnNames, column.Name)
				if index == -1 {
					if column.NotNull {
						// typeに応じて綺麗に対応する方法を考える（デフォルト値対応も）
						if column.Type == "datetime" {
							value = append(value, time.Now().Format("2006-01-02 15:04:05"))
						} else {
							value = append(value, "")
						}
					} else {
						value = append(value, "NULL")
					}
					continue
				}
				value = append(value, row[index+1].(string))
			}
			values = append(values, value)
		default:
			// FIXME: support tag
			continue
		}
	}

	// Create output data
	columns := make([]string, 0)
	for _, column := range sc.Columns {
		columns = append(columns, column.Name)
	}
	//values = append([][]string{columns}, values...) // TODO: 遅いので修正する（https://mattn.kaoriya.net/software/lang/go/20150928144704.htm）

	rows := &Rows{
		Columns: columns,
		Values:  values,
	}
	return rows, nil
}

func getClientSecretJSONPath() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("no caller information")
	}
	return filepath.Dir(filename) + "/../resources/spreadsheets/client_secret.json", nil
}

func getService() *sheets.Service {
	ctx := context.Background()

	jsonPath, err := getClientSecretJSONPath()
	if err != nil {
		log.Fatalln(err)
	}
	b, err := ioutil.ReadFile(jsonPath)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved credentials
	// at ~/.credentials/sheets.googleapis.com-go-quickstart.json
	config, err := google.ConfigFromJSON(b, "https://www.googleapis.com/auth/spreadsheets")
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

// GetClient uses a Context and Config to retrieve a Token
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

// getTokenFromWeb uses Config to request a Token.
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
