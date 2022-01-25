package neo4j

// ConcatenateStringSlices is a helper method to flatten a [][]string slice into a []sting slice.
// This can be useful for combining bookmarks of multiple sessions.
func ConcatenateStringSlices(slices... []string) []string  {
	var lenSum int
	for _, s := range slices {
		lenSum += len(s)
	}
	res := make([]string, lenSum)
	var i int
	for _, s := range slices {
		i += copy(res[i:], s)
	}
	return res
}
