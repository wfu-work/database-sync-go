package webs

import _ "embed"

//go:embed database-sync-web.zip
var staticFile []byte

func Static() []byte {
	return staticFile
}
