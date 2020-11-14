// Package test for the application
package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/ddliu/go-httpclient"
	"github.com/rs/zerolog/log"
)

// Client holds the info for the HTTP client
type Client struct {
	ServerURL string
	http      *httpclient.HttpClient
	Resp      *httpclient.Response
	JSON      map[string]interface{}
	Text      string
	Err       error
}

// NewClient creates a new HTTP client
func NewClient(serverURL string) *Client {
	return &Client{
		http:      httpclient.NewHttpClient(),
		ServerURL: serverURL,
	}
}

// URLFor creates an URL with a path
func (c *Client) URLFor(path string) string {
	return fmt.Sprintf("%s%s", c.ServerURL, path)
}

// Get sends a GET request
func (c *Client) Get(path string) {
	url := c.URLFor(path)
	res, err := c.http.Get(url)
	c.Resp = res
	c.Err = err
	c.parseResponse()
}

// Delete sends a DELETE request
func (c *Client) Delete(path string) {
	url := c.URLFor(path)
	res, err := c.http.Delete(url)
	c.Resp = res
	c.Err = err
	c.parseResponse()
}

// Post sends a POST request
func (c *Client) Post(path, data string) {
	url := c.URLFor(path)
	res, err := c.http.PostJson(url, data)
	c.Resp = res
	c.Err = err
	c.parseResponse()
}

// Put sends a PUT request
func (c *Client) Put(path, data string) {
	url := c.URLFor(path)
	res, err := c.http.PutJson(url, data)
	c.Resp = res
	c.Err = err
	c.parseResponse()
}

func (c *Client) parseResponse() {
	if c.Err != nil {
		return
	}

	if c.Resp != nil && c.Resp.Body != nil {
		bytes, err := ioutil.ReadAll(c.Resp.Body)
		if err != nil {
			log.Info().Msg("Could not read bytes from http response")
		} else {
			if c.HasJSON() {
				c.maybeParseJSON(bytes)
			} else if c.HasText() {
				c.maybeParseText(bytes)
			}
		}
	}
}

func (c *Client) maybeParseJSON(bytes []byte) {
	var anyJSON map[string]interface{}
	err := json.Unmarshal(bytes, &anyJSON)
	if err != nil {
		log.Info().Str("Could not unmarshal json: %s", string(bytes))
	} else {
		c.JSON = anyJSON
	}
}

func (c *Client) maybeParseText(bytes []byte) {
	c.Text = string(bytes)
}

// HasJSON checks if content type is application/json
func (c *Client) HasJSON() bool {
	return c.Resp != nil && strings.Contains(c.Resp.Header.Get("content-type"), "application/json")
}

// HasText checks if content type is text/plain
func (c *Client) HasText() bool {
	return c.Resp != nil && strings.Contains(c.Resp.Header.Get("content-type"), "text/plain")
}
