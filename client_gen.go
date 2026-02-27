package daggo

import (
	"os"
	"path/filepath"

	"github.com/swetjen/virtuous/rpc"
)

const frontendClientPath = "frontend-web/api/client.gen.js"

func WriteFrontendClient(router *rpc.Router) error {
	if router == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(frontendClientPath), 0755); err != nil {
		return err
	}
	return router.WriteClientJSFile(frontendClientPath)
}
