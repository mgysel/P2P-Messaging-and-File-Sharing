export GLOG = warn
export BINLOG = warn
export HTTPLOG = warn

test: test_hw0 test_hw1 test_hw2

xtest: setbin test

setbin:
	cd gui/; \
	go build -race -o node; \
	cp node ../peer/tests/integration

test_hw0: test_unit_hw0 test_int_hw0
test_hw1: test_unit_hw1 test_int_hw1
test_hw2: test_unit_hw2 test_int_hw2

test_unit_hw0:
	go test -failfast -v -race -run Test_HW0 ./peer/tests/unit

test_unit_hw1:
	go test -failfast -v -race -run Test_HW1 ./peer/tests/unit

test_unit_hw2:
	go test -failfast -v -race -run Test_HW2 ./peer/tests/unit

test_int_hw0:
	go test -failfast -timeout 40m -v -race -run Test_HW0 ./peer/tests/integration

test_int_hw1:
	go test -failfast -timeout 40m -v -race -run Test_HW1 ./peer/tests/integration

test_int_hw2:
	go test -failfast -timeout 5m -v -race -run Test_HW2 ./peer/tests/integration

lint:
	# Coding style static check.
	@go get -v honnef.co/go/tools/cmd/staticcheck
	@go mod tidy
	staticcheck ./...

vet:
	go vet ./...
