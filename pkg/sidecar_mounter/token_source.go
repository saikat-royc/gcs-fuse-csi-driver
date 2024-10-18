package sidecarmounter

import (
	"golang.org/x/oauth2"
)

type TokenSource struct {
	token *oauth2.Token
}

func (ts *TokenSource) Token() (*oauth2.Token, error) {
	return ts.token, nil
}
