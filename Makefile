test:
	go test -v ./...
test.storage:
	go test -v ./storage
install:
	go install honnef.co/go/tools/cmd/staticcheck@v0.2.0
clean:
	@rm -f  **/*.png
lint:
	staticcheck ./...
