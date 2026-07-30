package handler

import (
	"strings"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/stretchr/testify/require"
)

func TestParseBlockListEnforcesProtocolEntryLimit(t *testing.T) {
	blockListAtLimit := blockListXML(cache.MaxBlockListEntries)
	blockIDs, err := parseBlockList(strings.NewReader(blockListAtLimit))
	require.NoError(t, err)
	require.Len(t, blockIDs, cache.MaxBlockListEntries)

	_, err = parseBlockList(strings.NewReader(
		blockListAtLimit[:len(blockListAtLimit)-len("</BlockList>")] + "<Latest>YQ==</Latest></BlockList>",
	))
	require.ErrorIs(t, err, cache.ErrBlockListTooLarge)
}

func blockListXML(entries int) string {
	return "<BlockList>" + strings.Repeat("<Latest>YQ==</Latest>", entries) + "</BlockList>"
}
