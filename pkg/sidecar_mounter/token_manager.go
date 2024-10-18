package sidecarmounter

import (
	"golang.org/x/oauth2"
)

type TokenManager interface {
	GetTokenSource(token *oauth2.Token) oauth2.TokenSource
}

type tokenManager struct{}

func NewTokenManager() TokenManager {
	return &tokenManager{}
}

func (tm *tokenManager) GetTokenSource(token *oauth2.Token) oauth2.TokenSource {
	return &TokenSource{
		token: token,
	}
}
