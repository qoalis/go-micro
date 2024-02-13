package tests

import (
	"github.com/qoalis/go-micro/micro"
	"os"
)

func UseInMemoryDatabase() {
	_ = os.Setenv(micro.DatabaseUrl, "file:__tenant__?mode=memory&cache=shared")
}
