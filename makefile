# Makefile for the nav rain app project

GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOMOD=$(GOCMD) mod
BINARY_NAME=databaseSyncApp
PACKAGE=./

# Default target executed when no arguments are given to make.
all: build

# Build the binary
build:
	$(GOBUILD) -o $(BINARY_NAME) $(PACKAGE)

# Clean the build files
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

# Run the application
run: build
	./$(BINARY_NAME)

# Tidy up the dependencies
tidy:
	$(GOMOD) tidy

.PHONY: all build clean run tidy