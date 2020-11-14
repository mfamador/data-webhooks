// Package processor for benthos processors
package processor

import (
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	processor.RegisterPlugin(
		"matcher",
		func() interface{} {
			return NewMatcherConfig()
		},
		func(
			iconf interface{},
			mgr types.Manager,
			logger log.Modular,
			stats metrics.Type,
		) (types.Processor, error) {
			conf, ok := iconf.(*MatcherConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return NewMatcher(*conf, logger, mgr)
		},
	)
	processor.DocumentPlugin(
		"matcher",
		`This plugin matches messages with subscription filters.`,
		nil,
	)
}

// MatcherConfig contains config fields for our plugin type.
type MatcherConfig struct {
	SubscribersServiceAddress string `json:"subscribers_service_address" yaml:"subscribers_service_address"`
}

// NewMatcherConfig creates a config with default values.
func NewMatcherConfig() *MatcherConfig {
	return &MatcherConfig{
		SubscribersServiceAddress: "",
	}
}

//------------------------------------------------------------------------------

// Matcher is a processor that matches incoming messages to subscriptions.
type Matcher struct {
	subscribersServiceAddress string
	log                       log.Modular
}

// NewMatcherConfig creates a new instance of ProcessCapabilities
func NewMatcher(conf MatcherConfig, logger log.Modular, _ types.Manager) (types.Processor, error) {
	pc := &Matcher{
		subscribersServiceAddress: conf.SubscribersServiceAddress,
		log:                       logger,
	}
	return pc, nil
}

// ProcessMessage applies the processor to a message
func (m *Matcher) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	// Always create a new copy if we intend to mutate message contents.
	newMsg := msg.Copy()

	proc := func(i int, span opentracing.Span, part types.Part) error {

		m.log.Infof("msg passing through matcher: %s", string(part.Get()))
		return nil
	}

	processor.IteratePartsWithSpan("matcher", []int{}, newMsg, proc)

	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *Matcher) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (m *Matcher) WaitForClose(_ time.Duration) error {
	return nil
}
