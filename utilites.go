package main

import (
	"encoding/json"
	"fmt"
	"regexp"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func performAuth(md metadata.MD, aclData map[string][]*regexp.Regexp, fullMethod string) error {
	consumer := md.Get("consumer")
	if len(consumer) < 1 {
		return grpc.Errorf(codes.Unauthenticated, "no consumer info provided")
	}
	paths, ok := aclData[consumer[0]]
	if !ok {
		return grpc.Errorf(codes.Unauthenticated, "unknown consumer")
	}

	// validate path permission
	var isGranted bool
	for _, path := range paths {
		if path.MatchString(fullMethod) {
			isGranted = true
			break
		}
	}
	if !isGranted {
		return grpc.Errorf(codes.Unauthenticated, "access to path denied")
	}
	return nil
}

func parseAclDataToRegextp(inputAclData string) (map[string][]*regexp.Regexp, error) {
	var aclData map[string][]string
	err := json.Unmarshal([]byte(inputAclData), &aclData)

	if err != nil {
		return nil, fmt.Errorf("Error to parse aclData: %w", err)
	}
	aclPaths := make(map[string][]*regexp.Regexp, len(aclData))

	for key, pathList := range aclData {
		regexPathList := make([]*regexp.Regexp, len(pathList))
		for i, path := range pathList {
			regexPathList[i] = regexp.MustCompile(path)
		}
		aclPaths[key] = regexPathList
	}

	return aclPaths, nil
}
