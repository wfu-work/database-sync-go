package webs

import _ "embed"

//go:embed navmesh-web.zip
var staticFile []byte

func Static() []byte {
	return staticFile
}
