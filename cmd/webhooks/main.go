// main package for webhooks application
package main

import (
	"github.com/Jeffail/benthos/v3/lib/service"
	_ "github.com/mfamador/datawebhooks/processor"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Starting Data Webhooks")

	service.Run()
}
