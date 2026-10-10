package portal_test

import (
	"context"
	"fmt"
	"log"
	"net/http/httptest"

	"github.com/t3hk0d3/go-loqed/cloud/portal"
)

func ExampleClient_Login() {
	// A fake portal (see fake_test.go) with the account me@example.com.
	srv := httptest.NewServer(&fakePortal{version: "v1", email: "me@example.com", password: "s3cret", sessions: map[string]*fakeSession{}})
	defer srv.Close()

	// A real client needs no options: portal.New().
	c := portal.New(portal.WithBaseURL(srv.URL))
	ctx := context.Background()

	s, err := c.Login(ctx, "me@example.com", "s3cret")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = s.Logout(ctx) }()

	// The value is shown only once: store it, then use it with cloud.New.
	tok, err := s.CreateToken(ctx, "my-integration")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("created", tok.Name, tok.Value != "")

	tokens, err := s.ListTokens(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range tokens {
		fmt.Println("listed", t.Name, t.ID == tok.ID)
	}
	// Output:
	// created my-integration true
	// listed my-integration true
}
