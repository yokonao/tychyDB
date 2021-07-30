test:
	go test -v ./...
test_storage:
	go test -v ./storage
install:
	go install honnef.co/go/tools/cmd/staticcheck@v0.2.0
lint:
	staticcheck ./...
