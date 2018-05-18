package main

import (
	"encoding/json"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"log"
	"os"
)


type DummyStruct struct {
	testJson []byte
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(oauth2.NoContext, authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

const (
	jsonKey = `{"installed":{"client_id":"869271934593-b071s6ndguech622u4u5sc95n1ukos6u.apps.googleusercontent.com","project_id":"morning-tide","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://accounts.google.com/o/oauth2/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"_FyFx-PO8E0UMDtto-oR-wrY","redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]}}`
)

func main() {
	b := []byte(jsonKey)

	// If modifying these scopes, delete your previously saved client_secret.json.
	config, err := google.ConfigFromJSON(b, "https://www.googleapis.com/auth/spreadsheets.readonly")
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	tok := getTokenFromWeb(config)
	json.NewEncoder(os.Stdout).Encode(tok)

	encJsonKey := "12344"
	fmt.Println(encJsonKey)
}
