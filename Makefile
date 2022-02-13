test:
	go test -p=1 -v ./...
test.storage:
	go test -v -p=1 ./storage
install:
	go install honnef.co/go/tools/cmd/staticcheck@v0.2.0
clean:
	@rm -f  **/*.png
lint:
	staticcheck ./...
