test:
	go test -v ./...
test.storage:
	go test -v ./storage
test.btree:
	go test -v ./btree
test.bptree:
	go test -v ./bptree
install:
	go install honnef.co/go/tools/cmd/staticcheck@v0.2.0
clean:
	@rm -f  **/*.png
lint:
	staticcheck ./...
